package rabbitmqqueue_test

import (
	"context"
	"fmt"

	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

func ExampleCredentialProviderFunc() {
	provider := rabbitmqqueue.CredentialProviderFunc(
		func(context.Context) (rabbitmqqueue.Credentials, error) {
			return rabbitmqqueue.Credentials{
				Username: "orders",
				Password: []byte("resolved-attempt-secret"),
			}, nil
		},
	)

	credentials, err := provider.Credentials(context.Background())
	fmt.Println(credentials.Username, err)
	clear(credentials.Password)

	// Output: orders <nil>
}
