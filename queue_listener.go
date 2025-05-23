package queues

import (
	"context"
	"fmt"
	"time"

	".../internal/api"
	hlp ".../internal/helpers"
	stg ".../internal/storage/claims"
	cfg ".../pkg/config"
	lgr ".../pkg/logger"

	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
	st ".../statuses"
)

type claimsRequest struct {
	Id                  uint64         `json:"Id"`
	ExternalId          uint64         `json:"ExternalId"`
	ClaimCauseId        uint64         `json:"CauseId"`
	Description         string         `json:"Descripion"`
	CurrencyId          uint64         `json:"CurrencyId"`
	CurrencyValue       float64        `json:"CurrencyValue"`
	StatusCode          uint64         `json:"StatusCode"`
	CouponCode          string         `json:"CouponCode"`
	CouponDiscount      float64        `json:"CouponDiscount"`
	IsCouponUsed        bool           `json:"IsCouponUsed"`
	CouponDateStart     string         `json:"CouponDateStart"`
	CouponDateEnd       string         `json:"CouponDateEnd"`
	OrganizationId      uint64         `json:"OrganizationId"`
	ParcelContentListId uint64         `json:"ParcelContentListId"`
	GroupId             uint64         `json:"GroupId"`
	ClaimNum            string         `json:"ClaimNum"`
	Content             []claimProduct `json:"Content"`
	TotalSum            float64        `json:"TotalSum"`
	SumToRefund         float64        `json:"SumToRefund"`
	ClientId            uint64         `json:"ClientId"`
	SolutionId          uint64         `json:"SolutionId"`
	Comment             string         `json:"Comment"`
	SolutionDate        string         `json:"SolutionDate"`
	WorkingYear         uint64         `json:"WorkingYear"`

	ProcessingAttempts int `json:"ProcessingAttempts"`
}

type claimProduct struct {
	Id                uint64  `json:"Id"`
	ProductId         uint64  `json:"ProductId"`
	Quantity          uint64  `json:"Quantity"`
	QuantityCompleted uint64  `json:"QuantityCompleted"`
	Price             float64 `json:"Price"`
	IsDefective       bool    `json:"IsDefective"`
	FbId              uint64  `json:"FbId"`
}

func ReceiveNewClaims() {
	const (
		queueName = "new_claim"
	)

	// Подключение к RabbitMQ
	conn, err := amqp.Dial("amqp://" + cfg.CFG.RabbitMq.User + ":" + cfg.CFG.RabbitMq.Password + "@" + cfg.CFG.RabbitMq.BindAddr + "/")
	if err != nil {
		lgr.LOG.Error("_ERR_: ", "Failed to connect to RabbitMQ: "+err.Error())
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		lgr.LOG.Error("_ERR_: ", "Failed to open a channel: "+err.Error())
	}
	defer ch.Close()

	// Объявление отложенного exchange (должно совпадать с отправителем)
	err = ch.ExchangeDeclare(
		"delayed_exchange",
		"x-delayed-message",
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-delayed-type": "direct",
		},
	)
	if err != nil {
		lgr.LOG.Error("_ERR_: ", "Ошибка объявления exchange: "+err.Error())
	}

	// Объявление очереди
	q, err := ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
	)
	if err != nil {
		lgr.LOG.Error("_ERR_: ", "Ошибка объявления очереди: "+err.Error())
	}

	// Привязка очереди к exchange
	err = ch.QueueBind(
		q.Name,
		"delayed_routing",
		"delayed_exchange",
		false,
		nil,
	)
	if err != nil {
		lgr.LOG.Error("_ERR_: ", "Ошибка привязки очереди: "+err.Error())
	}

	// Настройка потребителя
	msgs, err := ch.Consume(
		q.Name,
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		lgr.LOG.Error("_ERR_: ", "Failed to register a consumer: "+err.Error())
	}

	lgr.LOG.Info("_INFO_ ", "Serving new_claim listener...")

	// Обработка сообщений
	for d := range msgs {
		lgr.LOG.Infof("Received a message: %v", d.Body)

		currentTime := time.Now()
		formattedTime := currentTime.Format("2006-01-02 15:04:05.000000")

		var request claimsRequest
		// Декодирование JSON-строки в структуру
		err := json.Unmarshal([]byte(d.Body), &request)
		if err != nil {
			// Не получилось декодировать сообщение. Создаём ошибку и удаляем из очереди
			fmt.Println("Ошибка при декодировании JSON:", err)

			_, stat := hlp.CreateError(context.Background(), "999", "Ошибка при декодировании JSON: "+err.Error(), formattedTime, request)
			if stat.Code == 100 {
				// Получилось создать ошибку. Удаляем сообщение и завершаем итерацию
				lgr.LOG.Info("_info_: ", "Error created. Message removed from queue")
			}

			_ = d.Ack(false) //ПОДТВЕРЖДЕНИЕ ПОЛУЧЕНИЯ
			continue
		}

		var (
			status  st.ResponseStatus
			content []*api.ClaimsProductsRequest
		)

		for _, product := range request.Content {
			content = append(content, &api.ClaimsProductsRequest{
				ProductId:         product.ProductId,
				Quantity:          product.Quantity,
				QuantityCompleted: product.QuantityCompleted,
				Price:             product.Price,
				IsDefective:       hlp.ToApiBool(&product.IsDefective),
				FbId:              product.FbId,
			})
		}

		_, status = stg.CreateClaim(context.Background(), &api.ClaimsRequest{
			AuthToken:           cfg.CFG.Server.CliToken,
			AuthorId:            cfg.CFG.Server.InternalAuthorId,
			CauseId:             request.ClaimCauseId,
			Description:         request.Description,
			CurrencyId:          request.CurrencyId,
			CurrencyValue:       request.CurrencyValue,
			StatusCode:          request.StatusCode,
			CouponCode:          request.CouponCode,
			CouponDiscount:      request.CouponDiscount,
			IsCouponUsed:        hlp.ToApiBool(&request.IsCouponUsed),
			CouponDateStart:     request.CouponDateStart,
			CouponDateEnd:       request.CouponDateEnd,
			ExternalId:          request.ExternalId,
			OrganizationId:      request.OrganizationId,
			ParcelContentListId: request.ParcelContentListId,
			GroupId:             request.GroupId,
			ClaimNum:            request.ClaimNum,
			Content:             content,
			ClientId:            request.ClientId,
			TotalSum:            request.TotalSum,
			SumToRefund:         request.SumToRefund,
			SolutionId:          request.SolutionId,
			Comment:             request.Comment,
			SolutionDate:        request.SolutionDate,
			WorkingYear:         request.WorkingYear,
		})

		if status.Code == 100 {
			_ = d.Ack(false) //ПОДТВЕРЖДЕНИЕ ПОЛУЧЕНИЯ
			lgr.LOG.Info("_info_: ", "Message was successfully recieved and processed")
			continue
		}

		lgr.LOG.Error("_ERR_: ", "Message processing error: "+status.Description)

		if request.ProcessingAttempts > 5 {
			// Было более пяти попыток обработать сообщение. Пробуем создать ошибку
			_, stat := hlp.CreateError(context.Background(), string(status.Code), status.Description, formattedTime, request)
			if stat.Code == 100 {
				// Получилось создать ошибку. Удаляем сообщение и завершаем итерацию
				lgr.LOG.Info("_info_: ", "Error created. Message removed from queue")
			}

			_ = d.Ack(false) //ПОДТВЕРЖДЕНИЕ ПОЛУЧЕНИЯ
			continue
		}

		request.ProcessingAttempts++        // Двигаем счётчик количества попыток обработки
		jsonReq, _ := json.Marshal(request) // Нет смысла обрабатывать ошибку, т.к. выше уже декодировали этот json

		code, description := hlp.SendMessage(queueName, jsonReq, 5*time.Minute) // Пробуем перезаписать сообщение
		if code == 100 {
			// Получилось перезаписать сообщение. Завершаем итерацию
			_ = d.Ack(false) //ПОДТВЕРЖДЕНИЕ ПОЛУЧЕНИЯ
			continue
		}
		lgr.LOG.Error("_ERR_: ", "Error sending message: "+description)

		// Перезаписать сообщение не вышло. Пробуем создать ошибку
		_, stat := hlp.CreateError(context.Background(), string(status.Code), status.Description, formattedTime, request)
		if stat.Code == 100 {
			// Получилось создать ошибку. Удаляем сообщение и завершаем итерацию
			lgr.LOG.Info("_info_: ", "Error created. Message removed from queue")
		}

		_ = d.Ack(false) //ПОДТВЕРЖДЕНИЕ ПОЛУЧЕНИЯ
	}
}
