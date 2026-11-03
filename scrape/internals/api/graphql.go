package twitterAPI

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/jeetdotso/tweet-indexer-service/config"
	"github.com/tidwall/gjson"
)

// Operation is one x.com GraphQL call: its query id plus the feature flags the
// web app sends with it. Values come from config/env/app.yaml (section x) so
// they can be updated when X redeploys, without touching Go code.
type Operation struct {
	Name         string
	QueryID      string
	Features     map[string]bool
	FieldToggles map[string]bool
}

var (
	opsMu sync.RWMutex
	ops   = map[string]Operation{}
)

// Configure loads the operation table from config. Calling it again replaces
// the table. X_QUERY_ID_<KEY> in the environment overrides a query id.
func Configure(x config.XConfig) {
	next := map[string]Operation{}
	for key, o := range x.Operations {
		op := Operation{Name: o.Name, QueryID: o.QueryID, Features: copyFlags(o.Features), FieldToggles: copyFlags(o.FieldToggles)}
		if v := strings.TrimSpace(os.Getenv("X_QUERY_ID_" + strings.ToUpper(key))); v != "" {
			op.QueryID = v
		}
		next[strings.ToLower(key)] = op
	}
	opsMu.Lock()
	ops = next
	opsMu.Unlock()
}

// SetOperation registers one operation by hand (tests, tools).
func SetOperation(key string, op Operation) {
	opsMu.Lock()
	ops[strings.ToLower(key)] = op
	opsMu.Unlock()
}

func operation(key string) (Operation, error) {
	opsMu.RLock()
	defer opsMu.RUnlock()
	op, ok := ops[strings.ToLower(key)]
	if !ok || op.QueryID == "" || op.Name == "" {
		return Operation{}, fmt.Errorf("x graphql operation %q is not configured (config section x.operations)", key)
	}
	return op, nil
}

func copyFlags(in map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// graphQL runs a configured GET operation. If X rejects the call because the
// feature list is missing flags, they are added as false and the call is made
// once more; the fix is also kept in memory so later calls do not repeat it.
func (tc *TwitterClient) graphQL(key string, variables map[string]interface{}) (*gjson.Result, error) {
	op, err := operation(key)
	if err != nil {
		return nil, err
	}
	for attempt := 0; ; attempt++ {
		res, err := tc.graphQLOnce(op, variables)
		ae, ok := AsAPIError(err)
		if err == nil || !ok || len(ae.MissingFeatures) == 0 || attempt >= 1 {
			return res, err
		}
		for _, f := range ae.MissingFeatures {
			if _, present := op.Features[f]; !present {
				op.Features[f] = false
			}
		}
		opsMu.Lock()
		ops[strings.ToLower(key)] = op
		opsMu.Unlock()
	}
}

func (tc *TwitterClient) graphQLOnce(op Operation, variables map[string]interface{}) (*gjson.Result, error) {
	vars, _ := json.Marshal(variables)
	features, _ := json.Marshal(op.Features)
	params := url.Values{}
	params.Add("variables", string(vars))
	params.Add("features", string(features))
	if len(op.FieldToggles) > 0 {
		toggles, _ := json.Marshal(op.FieldToggles)
		params.Add("fieldToggles", string(toggles))
	}
	return tc.commonRequest(fmt.Sprintf("/i/api/graphql/%s/%s", op.QueryID, op.Name), params, nil)
}
