package handler

import (
	"context"
	"github.com/LeezyWannaFall/Go-Search-Trends/internal/model"
)

type TrendingServiceHandler interface {
    GetTop(ctx context.Context, n int) []model.TopEntry
}