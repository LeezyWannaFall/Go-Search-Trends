package model

type SearchEvent struct {
    Query     string `json:"query"`
    UserID    string `json:"user_id"`
    Timestamp string `json:"timestamp"`
}

type TopEntry struct {
    Query string `json:"query"`
    Count int    `json:"count"`
}