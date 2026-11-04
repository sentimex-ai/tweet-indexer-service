package followscrape

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeetdotso/tweet-indexer-service/scrape/xclient"
	uuid "github.com/satori/go.uuid"
)

// Admin exposes on-demand scraping over HTTP:
//
//	POST /admin/following/scrape   {"handles":["a","b"],"resume":false,"prune":true}  -> 202
//	GET  /admin/following/batches/:id
//	GET  /admin/following/runs?limit=20
//
// Requests need header X-Admin-Token. Batches run one at a time on a single
// worker so concurrent requests cannot multiply the load on the pool.
type Admin struct {
	deps  *Deps
	token string
	queue chan *Batch
	mu    sync.Mutex
	batch map[string]*Batch
}

type Batch struct {
	ID       string    `json:"id"`
	Handles  []string  `json:"handles"`
	Status   string    `json:"status"` // queued, running, done
	Queued   time.Time `json:"queued_at"`
	Finished time.Time `json:"finished_at,omitempty"`
	Runs     []*Run    `json:"runs"`
	resume   bool
	prune    bool
}

func NewAdmin(ctx context.Context, deps *Deps, token string) *Admin {
	a := &Admin{deps: deps, token: token, queue: make(chan *Batch, 64), batch: map[string]*Batch{}}
	go a.worker(ctx)
	return a
}

func (a *Admin) Register(r gin.IRouter) {
	g := r.Group("/admin/following", a.auth)
	g.POST("/scrape", a.scrape)
	g.GET("/batches/:id", a.getBatch)
	g.GET("/runs", a.runs)
}

func (a *Admin) auth(c *gin.Context) {
	got := c.GetHeader("X-Admin-Token")
	if a.token == "" || subtle.ConstantTimeCompare([]byte(got), []byte(a.token)) != 1 {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing or bad X-Admin-Token"})
		return
	}
	c.Next()
}

func (a *Admin) scrape(c *gin.Context) {
	var req struct {
		Handles []string `json:"handles"`
		Resume  bool     `json:"resume"`
		Prune   *bool    `json:"prune"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var hs []string
	seen := map[string]bool{}
	for _, h := range req.Handles {
		h = strings.ToLower(xclient.NormalizeHandle(h))
		if h != "" && !seen[h] {
			seen[h] = true
			hs = append(hs, h)
		}
	}
	if len(hs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "handles is empty"})
		return
	}
	b := &Batch{ID: uuid.NewV4().String(), Handles: hs, Status: "queued", Queued: time.Now(), resume: req.Resume, prune: req.Prune == nil || *req.Prune}
	a.mu.Lock()
	a.batch[b.ID] = b
	a.mu.Unlock()
	select {
	case a.queue <- b:
		c.JSON(http.StatusAccepted, b)
	default:
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "queue full"})
	}
}

func (a *Admin) getBatch(c *gin.Context) {
	a.mu.Lock()
	b, ok := a.batch[c.Param("id")]
	var snap Batch
	if ok {
		snap = *b // copy under lock; the worker mutates b
	}
	a.mu.Unlock()
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "unknown batch (batches live in memory; see /runs)"})
		return
	}
	c.JSON(http.StatusOK, snap)
}

func (a *Admin) runs(c *gin.Context) {
	limit, _ := strconv.ParseInt(c.DefaultQuery("limit", "20"), 10, 64)
	rs, err := a.deps.Runs.Recent(c.Request.Context(), limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, rs)
}

func (a *Admin) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case b := <-a.queue:
			a.mu.Lock()
			b.Status = "running"
			a.mu.Unlock()
			opts := a.deps.Options()
			opts.Resume, opts.Prune = b.resume, b.prune
			runs := a.deps.NewScraper(opts).ScrapeAll(ctx, b.Handles)
			a.mu.Lock()
			b.Runs, b.Status, b.Finished = runs, "done", time.Now()
			a.mu.Unlock()
		}
	}
}
