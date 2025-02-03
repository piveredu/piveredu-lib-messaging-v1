# PiverEdu EventBus Go Client

Tell golang about your private organization
```bash
go env -w GOPRIVATE=github.com/piveredu
```

Installing package module
```bash
go get -u github.com/piveredu/piveredu-lib-messaging-v1
```

Creating a new EventBus client to dispatch messages
```go
client := eventbus.New()
```

Publishing a message within the microservice is a two (2) step process.

1. Create the message event you want to publish.
2. Call the publish method on the client to send the message.


```go
event := eventbus.NewEvent("tenant", "tenant.create", "tenant.create")
event.Payload = map[string]any{
    "tenant": map[string]any{
        "id":                  "bb4ef24b-1699-4452-ad09-f284e57c6049",
        "name":                "Acme School",
        "description":         "contact@acme.com",
    },
}
event.Metadata = map[string]any{
    "triggered_by": "bb4ef24b-1699-4452-ad09-f284e57c6049",
}

err := client.Publish("piveredu.tenant", event)
if err != nil {
    log.Fatalln("failed to send message: |", err)
}
```

Consuming message events from any microservice

```go
messages, err := client.Consume("piveredu.tenant")
if err != nil {
    log.Println("failed to consume messages :::::: |", err)
}

for message := range messages {
    fmt.Println("Message received :::::: |", message)
}
```

PS: Check the example folder for a full code walkthrough