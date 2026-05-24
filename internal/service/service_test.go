package service_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/LeezyWannaFall/Go-Search-Trends/internal/model"
	"github.com/LeezyWannaFall/Go-Search-Trends/internal/service"
)

// helpers

func makeEvent(query, userID string, ts time.Time) model.SearchEvent {
	return model.SearchEvent{Query: query, UserID: userID, Timestamp: ts}
}

func topEqual(a, b []model.TopEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsQuery(top []model.TopEntry, query string) bool {
	for _, e := range top {
		if e.Query == query {
			return true
		}
	}
	return false
}

func sortedStrings(ss []string) []string {
	out := make([]string, len(ss))
	copy(out, ss)
	sort.Strings(out)
	return out
}

func strSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// GetTop

func TestGetTop(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	tests := []struct {
		name   string
		events []model.SearchEvent
		n      int
		want   []model.TopEntry
	}{
		{
			name:   "пустой сервис возвращает nil",
			events: nil,
			n:      10,
			want:   nil,
		},
		{
			name:   "одно событие",
			events: []model.SearchEvent{makeEvent("golang", "u1", now)},
			n:      10,
			want:   []model.TopEntry{{Query: "golang", Count: 1}},
		},
		{
			name: "несколько запросов — сортировка по убыванию count",
			events: []model.SearchEvent{
				makeEvent("kafka",  "u1", now),
				makeEvent("golang", "u1", now),
				makeEvent("golang", "u2", now),
				makeEvent("golang", "u3", now),
				makeEvent("kafka",  "u2", now),
				makeEvent("docker", "u1", now),
			},
			n: 10,
			want: []model.TopEntry{
				{Query: "golang", Count: 3},
				{Query: "kafka",  Count: 2},
				{Query: "docker", Count: 1},
			},
		},
		{
			// docker < kafka алфавитно поэтому при n=2 в топ попадает docker
			name: "лимит n обрезает список",
			events: []model.SearchEvent{
				makeEvent("golang", "u1", now),
				makeEvent("golang", "u2", now),
				makeEvent("kafka",  "u1", now),
				makeEvent("docker", "u1", now),
			},
			n: 2,
			want: []model.TopEntry{
				{Query: "golang", Count: 2},
				{Query: "docker", Count: 1},
			},
		},
		{
			name: "при равном count — сортировка по алфавиту",
			events: []model.SearchEvent{
				makeEvent("zebra", "u1", now),
				makeEvent("apple", "u2", now),
				makeEvent("mango", "u3", now),
			},
			n: 10,
			want: []model.TopEntry{
				{Query: "apple", Count: 1},
				{Query: "mango", Count: 1},
				{Query: "zebra", Count: 1},
			},
		},
		{
			name: "событие старше 6 минут не попадает в топ",
			events: []model.SearchEvent{
				makeEvent("old", "u1", now.Add(-6*time.Minute)),
				makeEvent("new", "u1", now),
			},
			n:    10,
			want: []model.TopEntry{{Query: "new", Count: 1}},
		},
		{
			name: "n=1 возвращает только лидера",
			events: []model.SearchEvent{
				makeEvent("golang", "u1", now),
				makeEvent("golang", "u2", now),
				makeEvent("kafka",  "u1", now),
			},
			n:    1,
			want: []model.TopEntry{{Query: "golang", Count: 2}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := service.New()
			for _, e := range tt.events {
				svc.Add(ctx, e)
			}
			got := svc.GetTop(ctx, tt.n)
			if !topEqual(got, tt.want) {
				t.Errorf("GetTop() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Deduplication

func TestAdd_Deduplication(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	tests := []struct {
		name      string
		events    []model.SearchEvent
		query     string
		wantCount int
	}{
		{
			name: "один пользователь много раз — засчитывается как 1",
			events: []model.SearchEvent{
				makeEvent("golang", "u1", now),
				makeEvent("golang", "u1", now),
				makeEvent("golang", "u1", now),
			},
			query:     "golang",
			wantCount: 1,
		},
		{
			name: "разные пользователи — считается каждый",
			events: []model.SearchEvent{
				makeEvent("golang", "u1", now),
				makeEvent("golang", "u2", now),
				makeEvent("golang", "u3", now),
			},
			query:     "golang",
			wantCount: 3,
		},
		{
			name: "один пользователь в разных минутах — считается в каждом бакете",
			events: []model.SearchEvent{
				makeEvent("golang", "u1", now),
				makeEvent("golang", "u1", now.Add(-time.Minute)),
			},
			query:     "golang",
			wantCount: 2,
		},
		{
			name: "разные запросы одного пользователя не мешают друг другу",
			events: []model.SearchEvent{
				makeEvent("golang", "u1", now),
				makeEvent("kafka",  "u1", now),
				makeEvent("docker", "u1", now),
			},
			query:     "golang",
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := service.New()
			for _, e := range tt.events {
				svc.Add(ctx, e)
			}
			top := svc.GetTop(ctx, 10)
			for _, entry := range top {
				if entry.Query == tt.query {
					if entry.Count != tt.wantCount {
						t.Errorf("query %q: count = %d, want %d", tt.query, entry.Count, tt.wantCount)
					}
					return
				}
			}
			t.Errorf("query %q не найден в топе", tt.query)
		})
	}
}

// StopList

func TestStopList_Filtering(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	tests := []struct {
		name         string
		stopWords    []string
		events       []model.SearchEvent
		wantInTop    []string
		wantNotInTop []string
	}{
		{
			name:      "слово из стоп-листа не попадает в топ",
			stopWords: []string{"спам"},
			events: []model.SearchEvent{
				makeEvent("спам",   "u1", now),
				makeEvent("golang", "u1", now),
			},
			wantInTop:    []string{"golang"},
			wantNotInTop: []string{"спам"},
		},
		{
			name:      "пустой стоп-лист ничего не фильтрует",
			stopWords: nil,
			events: []model.SearchEvent{
				makeEvent("golang", "u1", now),
			},
			wantInTop:    []string{"golang"},
			wantNotInTop: nil,
		},
		{
			name:      "несколько слов в стоп-листе",
			stopWords: []string{"спам", "реклама"},
			events: []model.SearchEvent{
				makeEvent("спам",    "u1", now),
				makeEvent("реклама", "u2", now),
				makeEvent("golang",  "u3", now),
			},
			wantInTop:    []string{"golang"},
			wantNotInTop: []string{"спам", "реклама"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := service.New()
			for _, w := range tt.stopWords {
				svc.AddWord(ctx, w)
			}
			for _, e := range tt.events {
				svc.Add(ctx, e)
			}
			top := svc.GetTop(ctx, 10)
			for _, q := range tt.wantInTop {
				if !containsQuery(top, q) {
					t.Errorf("ожидалось %q в топе, но его нет. top=%v", q, top)
				}
			}
			for _, q := range tt.wantNotInTop {
				if containsQuery(top, q) {
					t.Errorf("ожидалось %q НЕ в топе, но оно есть. top=%v", q, top)
				}
			}
		})
	}
}

func TestStopList_RemoveWord(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	svc := service.New()
	svc.AddWord(ctx, "golang")

	// пока в стоп-листе — не должно быть в топе
	svc.Add(ctx, makeEvent("golang", "u1", now))
	if containsQuery(svc.GetTop(ctx, 10), "golang") {
		t.Fatal("слово из стоп-листа не должно быть в топе")
	}

	// удаляем из стоп-листа, добавляем новое событие
	svc.DeleteWord(ctx, "golang")
	svc.Add(ctx, makeEvent("golang", "u2", now))

	if !containsQuery(svc.GetTop(ctx, 10), "golang") {
		t.Error("после удаления из стоп-листа слово должно появляться в топе")
	}
}

// GetBlackList

func TestGetBlackList(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		addWords  []string
		delWords  []string
		wantWords []string
	}{
		{
			name:      "пустой стоп-лист",
			wantWords: []string{},
		},
		{
			name:      "добавление слов",
			addWords:  []string{"спам", "реклама"},
			wantWords: []string{"реклама", "спам"},
		},
		{
			name:      "добавление и удаление",
			addWords:  []string{"спам", "реклама"},
			delWords:  []string{"спам"},
			wantWords: []string{"реклама"},
		},
		{
			name:      "удаление несуществующего слова не паникует",
			delWords:  []string{"нет_такого"},
			wantWords: []string{},
		},
		{
			name:      "дублирующее добавление одного слова",
			addWords:  []string{"спам", "спам", "спам"},
			wantWords: []string{"спам"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := service.New()
			for _, w := range tt.addWords {
				svc.AddWord(ctx, w)
			}
			for _, w := range tt.delWords {
				svc.DeleteWord(ctx, w)
			}
			got := sortedStrings(svc.GetBlackList(ctx))
			want := sortedStrings(tt.wantWords)
			if !strSlicesEqual(got, want) {
				t.Errorf("GetBlackList() = %v, want %v", got, want)
			}
		})
	}
}

// Concurrent access

// запускать с флагом -race: go test -race ./internal/service/...
func TestConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	svc := service.New()

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n * 3)

	// параллельные записи
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			svc.Add(ctx, makeEvent("golang", fmt.Sprintf("u%d", i), now))
		}(i)
	}

	// параллельные чтения топа
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			svc.GetTop(ctx, 10)
		}()
	}

	// параллельные операции со стоп-листом
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			word := fmt.Sprintf("word%d", i%5)
			if i%2 == 0 {
				svc.AddWord(ctx, word)
			} else {
				svc.DeleteWord(ctx, word)
			}
		}(i)
	}

	wg.Wait()

	// после всех конкурентных операций сервис должен отвечать без паники
	top := svc.GetTop(ctx, 10)
	if top == nil {
		t.Log("топ пустой (некоторые события попали в стоплист) — это нормально")
	}
}
