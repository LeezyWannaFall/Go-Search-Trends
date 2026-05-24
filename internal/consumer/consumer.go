package consumer

import (
    "context"
    "encoding/json"
    "log"

    "github.com/segmentio/kafka-go"
    "github.com/LeezyWannaFall/Go-Search-Trends/internal/model"
)

type Consumer struct {
    reader  *kafka.Reader
    service TrendingServiceConsumer
}

func New(brokers []string, topic string, service TrendingServiceConsumer) *Consumer {
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers: brokers,
        Topic:   topic,
        GroupID: "trending-service",
    })

    return &Consumer{reader: reader, service: service}
}

func (c *Consumer) Run(ctx context.Context) {
    defer c.reader.Close()

    for {
        msg, err := c.reader.ReadMessage(ctx)	// блокируется пока не придёт новое сообщение
        if err != nil {
            if ctx.Err() != nil {
                return	// если контекст отменён - выходим
            }
            log.Printf("consumer error: %v", err)
            continue  // при других ошибках продолжаем читать
        }

        var event model.SearchEvent
        if err := json.Unmarshal(msg.Value, &event); err != nil {	// парсим json в searchevent
            log.Printf("failed to parse message: %v", err)
            continue
        }
        
        log.Printf("added event: query=%s", event.Query)
        c.service.Add(ctx, event)
    }
}