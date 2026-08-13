package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultHTTPAddr  = ":8080"
	defaultListLimit = 100
	maxListLimit     = 500
	kafkaTopic       = "answers.received"
)

type Config struct {
	HTTPAddr       string
	DatabaseURL    string
	KafkaBootstrap string
	LogLevel       logrus.Level
}

type Answer struct {
	tableName struct{} `pg:"answers"`

	ID         int64     `json:"id" pg:"id,pk"`
	ChatID     int64     `json:"chat_id" pg:"chat_id,notnull"`
	TelegramID int64     `json:"telegram_id" pg:"telegram_id,notnull"`
	Text       string    `json:"text" pg:"text,notnull,type:text"`
	SentAt     time.Time `json:"sent_at" pg:"sent_at,notnull,type:timestamptz"`
	CreatedAt  time.Time `json:"created_at" pg:"created_at,notnull,default:now()"`
}

type AnswerRequest struct {
	ChatID     int64  `json:"chat_id"`
	TelegramID int64  `json:"telegram_id"`
	Text       string `json:"text"`
	SentAt     string `json:"sent_at"`
}

type AnswerReceivedEvent struct {
	ChatID   int64  `json:"chat_id"`
	AnswerID int64  `json:"answer_id"`
	SentAt   string `json:"sent_at"`
}

type Service struct {
	db     *pg.DB
	kafka  *kgo.Client
	logger *logrus.Logger
}

func main() {
	logger := newLogger()

	cfg, err := loadConfig()
	if err != nil {
		logger.WithError(err).Fatal("configuration error")
	}

	db, err := connectDB(cfg.DatabaseURL)
	if err != nil {
		logger.WithError(err).Fatal("database connection error")
	}

	kafkaClient, err := newKafkaClient(cfg.KafkaBootstrap)
	if err != nil {
		_ = db.Close()
		logger.WithError(err).Fatal("kafka client error")
	}

	service := &Service{db: db, kafka: kafkaClient, logger: logger}
	if err := service.migrate(context.Background()); err != nil {
		kafkaClient.CloseAllowingRebalance()
		_ = db.Close()
		logger.WithError(err).Fatal("migration error")
	}

	e := echo.New()
	e.HideBanner = true
	e.HTTPErrorHandler = httpErrorHandler(logger)
	e.Use(requestLogger(logger))

	e.GET("/healthz", healthz)
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	e.POST("/answers", service.createAnswer)
	e.GET("/answers", service.listAnswers)

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           e,
		ReadHeaderTimeout: 5 * time.Second,
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGTERM, os.Interrupt)

	serverErr := make(chan error, 1)
	go func() {
		logger.WithField("addr", cfg.HTTPAddr).Info("http server listening")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	select {
	case sig := <-shutdown:
		logger.WithField("signal", sig.String()).Info("shutdown requested")
	case err := <-serverErr:
		if err != nil {
			logger.WithError(err).Fatal("http server error")
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.WithError(err).Error("http shutdown error")
	}

	if err := kafkaClient.Flush(shutdownCtx); err != nil {
		logger.WithError(err).Error("kafka flush error")
	}
	kafkaClient.CloseAllowingRebalance()

	if err := db.Close(); err != nil {
		logger.WithError(err).Error("database close error")
	}

	logger.Info("service stopped")
}

func loadConfig() (Config, error) {
	logLevel, err := parseLogLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		return Config{}, err
	}

	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}

	kafkaBootstrap := strings.TrimSpace(os.Getenv("KAFKA_BOOTSTRAP"))
	if kafkaBootstrap == "" {
		return Config{}, errors.New("KAFKA_BOOTSTRAP is required")
	}

	httpAddr := strings.TrimSpace(os.Getenv("HTTP_ADDR"))
	if httpAddr == "" {
		httpAddr = defaultHTTPAddr
	}

	return Config{
		HTTPAddr:       httpAddr,
		DatabaseURL:    databaseURL,
		KafkaBootstrap: kafkaBootstrap,
		LogLevel:       logLevel,
	}, nil
}

func parseLogLevel(raw string) (logrus.Level, error) {
	if raw == "" {
		return logrus.InfoLevel, nil
	}

	level, err := logrus.ParseLevel(strings.ToLower(strings.TrimSpace(raw)))
	if err != nil {
		return logrus.InfoLevel, fmt.Errorf("invalid LOG_LEVEL %q: %w", raw, err)
	}

	return level, nil
}

func newLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetOutput(os.Stdout)
	logger.SetReportCaller(false)

	level, err := parseLogLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		logger.SetLevel(logrus.InfoLevel)
		logger.WithError(err).Error("invalid LOG_LEVEL, using info")
	} else {
		logger.SetLevel(level)
	}

	return logger
}

func connectDB(databaseURL string) (*pg.DB, error) {
	opts, err := pg.ParseURL(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}

	opts.ApplicationName = "answers"
	db := pg.Connect(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.Ping(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return db, nil
}

func newKafkaClient(bootstrap string) (*kgo.Client, error) {
	seeds := splitCSV(bootstrap)
	if len(seeds) == 0 {
		return nil, errors.New("KAFKA_BOOTSTRAP has no brokers")
	}

	return kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(10*time.Millisecond),
	)
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	seedBrokers := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			seedBrokers = append(seedBrokers, part)
		}
	}
	return seedBrokers
}

func (s *Service) migrate(ctx context.Context) error {
	if err := s.db.ModelContext(ctx, (*Answer)(nil)).CreateTable(&orm.CreateTableOptions{IfNotExists: true}); err != nil {
		return fmt.Errorf("create answers table: %w", err)
	}

	const createIndexSQL = `CREATE INDEX IF NOT EXISTS idx_answers_chat_id_sent_at_desc ON answers (chat_id, sent_at DESC)`
	if _, err := s.db.ExecContext(ctx, createIndexSQL); err != nil {
		return fmt.Errorf("create answers index: %w", err)
	}

	return nil
}

func (s *Service) createAnswer(c echo.Context) error {
	var req AnswerRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid JSON body")
	}

	sentAt, err := time.Parse(time.RFC3339, req.SentAt)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "sent_at must be an RFC3339 timestamp")
	}

	if req.Text == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "text is required")
	}

	answer := &Answer{
		ChatID:     req.ChatID,
		TelegramID: req.TelegramID,
		Text:       req.Text,
		SentAt:     sentAt,
	}

	ctx, cancel := context.WithTimeout(c.Request().Context(), 10*time.Second)
	defer cancel()

	if err := s.db.RunInTransaction(ctx, func(tx *pg.Tx) error {
		_, err := tx.Model(answer).Insert()
		return err
	}); err != nil {
		s.logger.WithError(err).Error("insert answer")
		return echo.NewHTTPError(http.StatusInternalServerError, "insert answer failed")
	}

	if err := s.publishAnswerReceived(ctx, answer); err != nil {
		s.logger.WithError(err).WithField("answer_id", answer.ID).Error("publish answer event")
		return echo.NewHTTPError(http.StatusBadGateway, "publish answer event failed")
	}

	return c.JSON(http.StatusCreated, answer)
}

func (s *Service) publishAnswerReceived(ctx context.Context, answer *Answer) error {
	event := AnswerReceivedEvent{
		ChatID:   answer.ChatID,
		AnswerID: answer.ID,
		SentAt:   answer.SentAt.UTC().Format(time.RFC3339),
	}

	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	result := s.kafka.ProduceSync(ctx, &kgo.Record{
		Topic: kafkaTopic,
		Key:   []byte(strconv.FormatInt(answer.ChatID, 10)),
		Value: value,
	})

	if err := result.FirstErr(); err != nil {
		return fmt.Errorf("produce %s: %w", kafkaTopic, err)
	}

	return nil
}

func (s *Service) listAnswers(c echo.Context) error {
	chatIDRaw := strings.TrimSpace(c.QueryParam("chat_id"))
	if chatIDRaw == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "chat_id is required")
	}

	chatID, err := strconv.ParseInt(chatIDRaw, 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "chat_id must be an int64")
	}

	limit, err := parseLimit(c.QueryParam("limit"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	answers := make([]*Answer, 0, limit)
	ctx, cancel := context.WithTimeout(c.Request().Context(), 10*time.Second)
	defer cancel()

	if err := s.db.ModelContext(ctx, (*Answer)(nil)).
		Where("chat_id = ?", chatID).
		Order("sent_at desc").
		Limit(limit).
		Select(&answers); err != nil {
		s.logger.WithError(err).WithField("chat_id", chatID).Error("list answers")
		return echo.NewHTTPError(http.StatusInternalServerError, "list answers failed")
	}

	return c.JSON(http.StatusOK, answers)
}

func parseLimit(raw string) (int, error) {
	if raw == "" {
		return defaultListLimit, nil
	}

	limit, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.New("limit must be an integer")
	}
	if limit < 1 {
		return 0, errors.New("limit must be greater than zero")
	}
	if limit > maxListLimit {
		return maxListLimit, nil
	}

	return limit, nil
}

func healthz(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
}

func requestLogger(logger *logrus.Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			start := time.Now()
			err := next(c)

			logger.WithFields(logrus.Fields{
				"service_name": "answers",
				"method":       c.Request().Method,
				"uri":          c.Request().RequestURI,
				"status":       c.Response().Status,
				"latency":      time.Since(start).String(),
			}).WithError(err).Info("http_request")

			return err
		}
	}
}

func httpErrorHandler(logger *logrus.Logger) echo.HTTPErrorHandler {
	return func(err error, c echo.Context) {
		code := http.StatusInternalServerError
		message := "internal server error"

		var httpErr *echo.HTTPError
		if errors.As(err, &httpErr) {
			code = httpErr.Code
			if msg, ok := httpErr.Message.(string); ok {
				message = msg
			} else {
				message = fmt.Sprint(httpErr.Message)
			}
		} else {
			logger.WithError(err).Error("unhandled http error")
		}

		logger.WithFields(logrus.Fields{
			"service_name": "answers",
			"status":       code,
			"method":       c.Request().Method,
			"uri":          c.Request().RequestURI,
		}).WithError(err).Error("http_error")

		if writeErr := c.JSON(code, map[string]string{"error": message}); writeErr != nil {
			logger.WithError(writeErr).Error("write error response")
		}
	}
}
