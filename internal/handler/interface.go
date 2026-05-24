package handler

import (
	"context"
	"github.com/LeezyWannaFall/Go-Search-Trends/internal/model"
)

type TrendingServiceHandler interface {
    GetTop(ctx context.Context, n int) []model.TopEntry
	AddWord(ctx context.Context, word string)
	DeleteWord(ctx context.Context, word string)
	GetBlackList(ctx context.Context) []string
}