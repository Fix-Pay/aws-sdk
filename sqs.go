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

	result, err := ConsultSqs(svc, url)

	if err != nil {
		goutils.CreateFileDay(fmt.Sprint("Ocorreu um erro: '", err.Error(), "'\n no metodo LerFila(). \n"))
		return nil, err
	} else {
		if len(result.Messages) == 0 {
			fmt.Print("Aparentemente não há mensagens na fila => " + url + " \n")
			err = errors.New("Aparentemente não há mensagens na fila => " + url + " \n")
			return nil, err
		}
	}

	return result.Messages, err
}

func ConsultSqs(svc *sqs.SQS, url string) (*sqs.ReceiveMessageOutput, error) {
	result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
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
	})
	return result, err
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
		goutils.CreateFileDay(fmt.Sprint("Delete Error: ", err.Error()))
		return err
	} else {
		goutils.CreateFileDay(fmt.Sprint("Mensagem deletada com sucesso.", resultDelete))
		return nil
	}
	return err
}

func SendMessage(message string, estab string, usuario string, usuario_id string) (*sqs.SendMessageOutput, error) {
	messageenciodificada := goutils.EncodeStringToBase64(message)

	sess := session.Must(session.NewSession(&aws.Config{
		MaxRetries:                    aws.Int(1),
		CredentialsChainVerboseErrors: aws.Bool(true),
		HTTPClient:                    &http.Client{Timeout: 10 * time.Second},
	}))

	cfgs := aws.Config{}
	regian := "us-east-1"
	cfgs.Region = &regian

	svc := sqs.New(sess, &cfgs)

	messagesqsfixpay := "LinkPagItem|" + messageenciodificada + ";" + estab + ";" + usuario + ";" + usuario_id

	resultsend, err := svc.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: aws.Int64(10),

		MessageBody: aws.String(messagesqsfixpay),
		QueueUrl:    aws.String(goutils.Godotenv("ecsqs")),
	})

	return resultsend, err
}
