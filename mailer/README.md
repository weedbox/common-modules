# Mailer Module

An email sending module built on [Gomail](https://github.com/go-gomail/gomail), integrated with [Uber Fx](https://github.com/uber-go/fx) for dependency injection.

## Features

- Uber Fx dependency injection integration
- SMTP support with TLS
- Simple message composition API
- Configuration via Viper

## Installation

```bash
go get github.com/weedbox/common-modules/mailer
```

## Quick Start

### Basic Usage

```go
package main

import (
    "github.com/weedbox/common-modules/logger"
    "github.com/weedbox/common-modules/mailer"
    "go.uber.org/fx"
)

func main() {
    fx.New(
        logger.Module(),
        mailer.Module("mailer"),
    ).Run()
}
```

### Sending Email

```go
package notification

import (
    "context"

    "github.com/weedbox/common-modules/mailer"
    "go.uber.org/fx"
    "go.uber.org/zap"
)

type Params struct {
    fx.In

    Logger *zap.Logger
    Mailer *mailer.Mailer
}

type NotificationService struct {
    params Params
    logger *zap.Logger
}

func (s *NotificationService) SendWelcomeEmail(to, name string) error {
    msg := s.params.Mailer.NewMessage()

    msg.SetHeader("From", "noreply@example.com")
    msg.SetHeader("To", to)
    msg.SetHeader("Subject", "Welcome!")
    msg.SetBody("text/html", "<h1>Hello "+name+"!</h1><p>Welcome to our service.</p>")

    return s.params.Mailer.Send(msg)
}

func (s *NotificationService) SendEmailWithAttachment(to, subject, body, filePath string) error {
    msg := s.params.Mailer.NewMessage()

    msg.SetHeader("From", "noreply@example.com")
    msg.SetHeader("To", to)
    msg.SetHeader("Subject", subject)
    msg.SetBody("text/plain", body)
    msg.Attach(filePath)

    return s.params.Mailer.Send(msg)
}
```

## Configuration

Configuration is managed via Viper. All config keys are prefixed with the module's scope:

| Key | Default | Description |
|-----|---------|-------------|
| `{scope}.host` | `0.0.0.0` | SMTP server host |
| `{scope}.port` | `25` | SMTP server port |
| `{scope}.username` | `""` | SMTP username |
| `{scope}.password` | `""` | SMTP password |
| `{scope}.tls` | `false` | Enable TLS |

### TOML Configuration Example

```toml
[mailer]
host = "smtp.example.com"
port = 587
username = "user@example.com"
password = "secret"
tls = true
```

### Environment Variables Example

```bash
export MAILER_HOST=smtp.example.com
export MAILER_PORT=587
export MAILER_USERNAME=user@example.com
export MAILER_PASSWORD=secret
export MAILER_TLS=true
```

## API Reference

### Mailer

#### `Module(scope string) fx.Option`

Creates a Mailer module and returns an Fx Option.

- `scope`: Module namespace, used for config key prefix and logger naming

#### `NewMessage() *gomail.Message`

Creates a new email message.

```go
msg := mailer.NewMessage()
msg.SetHeader("From", "sender@example.com")
msg.SetHeader("To", "recipient@example.com")
msg.SetHeader("Subject", "Test Email")
msg.SetBody("text/plain", "This is a test email.")
```

#### `Send(msg *gomail.Message) error`

Sends an email message.

```go
err := mailer.Send(msg)
if err != nil {
    // Handle error
}
```

## Message Composition

### Plain Text Email

```go
msg := mailer.NewMessage()
msg.SetHeader("From", "sender@example.com")
msg.SetHeader("To", "recipient@example.com")
msg.SetHeader("Subject", "Plain Text Email")
msg.SetBody("text/plain", "This is plain text content.")
```

### HTML Email

```go
msg := mailer.NewMessage()
msg.SetHeader("From", "sender@example.com")
msg.SetHeader("To", "recipient@example.com")
msg.SetHeader("Subject", "HTML Email")
msg.SetBody("text/html", "<h1>Hello</h1><p>This is HTML content.</p>")
```

### Email with Attachment

```go
msg := mailer.NewMessage()
msg.SetHeader("From", "sender@example.com")
msg.SetHeader("To", "recipient@example.com")
msg.SetHeader("Subject", "Email with Attachment")
msg.SetBody("text/plain", "Please see the attached file.")
msg.Attach("/path/to/file.pdf")
```

### Multiple Recipients

```go
msg := mailer.NewMessage()
msg.SetHeader("From", "sender@example.com")
msg.SetHeader("To", "recipient1@example.com", "recipient2@example.com")
msg.SetHeader("Cc", "cc@example.com")
msg.SetHeader("Bcc", "bcc@example.com")
msg.SetHeader("Subject", "Email to Multiple Recipients")
msg.SetBody("text/plain", "Hello everyone!")
```

## License

Apache License 2.0
