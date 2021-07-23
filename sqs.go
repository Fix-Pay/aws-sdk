package aws_sdk

import (
	"errors"
	"fmt"
	goutils "github.com/armando-couto/goutils"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func ReadQueue(url string) ([]*sqs.Message, error) {
	svc := goutils.ConectionSQS()
	result, err := svc.ReceiveMessage(receive(url))

	if err != nil {
		goutils.CreateFileDayError(fmt.Sprint("Ocorreu um erro: '", err.Error(), "'\n no metodo LerFila(). \n"))
		return nil, err
	} else {
		if len(result.Messages) == 0 {
			err = errors.New("Aparentemente não há mensagens na fila => " + url + " \n")
			return nil, err
		}
	}

	return result.Messages, err
}

func ConsultSqs(svc *sqs.SQS, url string) (*sqs.ReceiveMessageOutput, error) {
	result, err := svc.ReceiveMessage(receive(url))
	return result, err
}

func receive(url string) *sqs.ReceiveMessageInput {
	return &sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            &url,
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(1), // 1 seconds
		WaitTimeSeconds:     aws.Int64(0),
	}
}

/*
	Removendo a fila
*/
func DeleteMessage(message *sqs.Message, url string) error {
	svc := goutils.ConectionSQS()

	resultDelete, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &url,
		ReceiptHandle: message.ReceiptHandle,
	})
	if err != nil {
		goutils.CreateFileDayError(fmt.Sprint("Delete Error: ", err.Error()))
		return err
	} else {
		goutils.CreateFileDayInfo(fmt.Sprint("Mensagem deletada com sucesso.", resultDelete))
		return nil
	}
	return err
}

func GenerateMessage(itens []interface{}) string {
	messagesqsfixpay := ""
	for _, item := range itens {
		messagesqsfixpay += item.(string)
	}
	return messagesqsfixpay
}

func SendMessage(message, queueUrl string) (*sqs.SendMessageOutput, error) {
	sess := session.Must(session.NewSession(&aws.Config{
		MaxRetries:                    aws.Int(1),
		CredentialsChainVerboseErrors: aws.Bool(true),
		HTTPClient:                    &http.Client{Timeout: 10 * time.Second},
	}))

	cfgs := aws.Config{}
	regian := "us-east-1"
	cfgs.Region = &regian

	svc := sqs.New(sess, &cfgs)
	resultsend, err := svc.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: aws.Int64(10),

		MessageBody: aws.String(message),
		QueueUrl:    aws.String(queueUrl),
	})

	return resultsend, err
}
