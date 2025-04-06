package telemetry

type Logger interface {
	Info(msg string)
	Debug(msg string)
	Error(msg string, err error)
}

type NOPLogger struct {
}

func (n NOPLogger) Info(msg string) {
}
func (n NOPLogger) Debug(msg string) {
}
func (n NOPLogger) Error(msg string, err error) {
}
