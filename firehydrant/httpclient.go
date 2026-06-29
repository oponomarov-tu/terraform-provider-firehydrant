package firehydrant

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/hashicorp/terraform-plugin-log/tflog"
	"golang.org/x/time/rate"
)

// This re-introduces the client-side rate limiting and 429 retry behavior that
// upstream added in PR #162 and reverted in PR #164. Unlike the reverted
// implementation it is an http.RoundTripper rather than a sling.Doer, so a
// single instance protects BOTH the firehydrant-go-sdk client and the legacy
// sling client. It also drops the coarse global mutex from #162 (which
// serialized every request); rate.Limiter is already safe for concurrent use.
const (
	// rateLimitWindow is the default bucket duration for client-side throttling.
	rateLimitWindow = 5 * time.Second
	// rateLimitRequests is the number of requests permitted per window (burst).
	rateLimitRequests = 10
	// maxRetries is the number of attempts made when the API returns HTTP 429.
	maxRetries = 5
	// defaultRetryBackoff is the wait between retries when no Retry-After header
	// is present on the 429 response.
	defaultRetryBackoff = 5 * time.Second
	// maxRetryBackoff caps how long we honor a Retry-After header, to avoid
	// blocking Terraform for an unbounded amount of time.
	maxRetryBackoff = 30 * time.Second
)

// rateLimitedTransport wraps an inner http.RoundTripper, applying a client-side
// rate limiter before each request and retrying requests that come back with
// HTTP 429 Too Many Requests.
type rateLimitedTransport struct {
	inner   http.RoundTripper
	limiter *rate.Limiter
	backoff time.Duration
}

var _ http.RoundTripper = (*rateLimitedTransport)(nil)

// newRateLimitedTransport builds a rateLimitedTransport around inner. If inner
// is nil, http.DefaultTransport is used.
func newRateLimitedTransport(inner http.RoundTripper) *rateLimitedTransport {
	if inner == nil {
		inner = http.DefaultTransport
	}
	return &rateLimitedTransport{
		inner:   inner,
		limiter: defaultRateLimiter(),
		backoff: defaultRetryBackoff,
	}
}

// defaultRateLimiter returns a limiter allowing rateLimitRequests per window,
// where the window defaults to rateLimitWindow but may be overridden with the
// FIREHYDRANT_RATE_LIMIT_SECONDS environment variable.
func defaultRateLimiter() *rate.Limiter {
	window := rateLimitWindow
	if v := os.Getenv("FIREHYDRANT_RATE_LIMIT_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			window = time.Duration(n) * time.Second
		}
	}
	// Spread the allowed requests evenly across the window (e.g. 10 requests per
	// 5s => one token every 500ms) while still permitting an initial burst.
	return rate.NewLimiter(rate.Every(window/rateLimitRequests), rateLimitRequests)
}

// RoundTrip implements http.RoundTripper.
func (t *rateLimitedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()

	var resp *http.Response
	var err error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if waitErr := t.limiter.Wait(ctx); waitErr != nil {
			return nil, fmt.Errorf("firehydrant: rate limiter wait failed: %w", waitErr)
		}

		// A consumed body cannot be safely resent, so only retry when it rewinds.
		if attempt > 0 && req.Body != nil && req.Body != http.NoBody {
			if req.GetBody == nil {
				return resp, err
			}
			body, gerr := req.GetBody()
			if gerr != nil {
				return resp, gerr
			}
			req.Body = body
		}

		resp, err = t.inner.RoundTrip(req)
		if err != nil {
			return resp, err
		}

		if resp.StatusCode != http.StatusTooManyRequests || attempt == maxRetries-1 {
			return resp, err
		}

		backoff := t.backoff
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if n, perr := strconv.Atoi(ra); perr == nil && n > 0 {
				if d := time.Duration(n) * time.Second; d < maxRetryBackoff {
					backoff = d
				} else {
					backoff = maxRetryBackoff
				}
			}
		}

		// Drain before close so the keep-alive connection can be reused.
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		tflog.Warn(ctx, "firehydrant: rate limited (HTTP 429), backing off before retry", map[string]any{
			"attempt": attempt + 1,
			"backoff": backoff.String(),
		})

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}

	return resp, err
}
