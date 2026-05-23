package consumer

import (
	"context"
	"github.com/LeezyWannaFall/Go-Search-Trends/internal/model"
)

type TrendingServiceConsumer interface {
    Add(ctx context.Context, event model.SearchEvent)
}