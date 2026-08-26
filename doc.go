// Package rabbitmqqueue provides bounded, RabbitMQ-native policy for AMQP
// 0-9-1 classic and quorum queues.
//
// The package deliberately keeps queue semantics such as exchanges, routing,
// publisher outcomes, manual settlement, queue types, and recovery visible. It
// does not provide a backend-neutral queue abstraction and does not claim
// exactly-once processing.
package rabbitmqqueue
