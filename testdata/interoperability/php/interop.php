<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use PhpAmqpLib\Connection\AMQPConnectionFactory;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

const EXPECTED_PHP_VERSION = '8.5.8';
const EXPECTED_AMQPLIB_VERSION = 'v3.7.4';
const EXPECTED_AMQPLIB_REFERENCE = '381b6f7c600e0e0c7463cdd7f7a1a3bc6268e5fd';
const PUBLISH_TOKEN_HEADER = 'x-rabbitmqqueue-publish-token';
const MAXIMUM_PUBLISH_TOKEN_BYTES = 149;
const MAXIMUM_CONFIGURATION_BYTES = 65536;
const MAXIMUM_CORPUS_BYTES = 65536;
const OPERATION_TIMEOUT_SECONDS = 15.0;

set_exception_handler(static function (Throwable $throwable): never {
    fwrite(STDERR, 'PHP_INTEROP_FAILED ' . $throwable::class . PHP_EOL);
    exit(1);
});

$autoload = getenv('PHP_AMQPLIB_AUTOLOAD');
$configurationPath = getenv('RABBITMQ_QUEUE_PHP_CONFIG');
$mode = $argv[1] ?? '';
$corpusPath = $argv[2] ?? dirname(__DIR__) . '/message-v1.json';
if (!is_string($autoload) || $autoload === '' || !is_file($autoload) ||
    !is_string($configurationPath) || $configurationPath === '' ||
    !in_array($mode, ['self-test', 'publish', 'consume', 'return'], true)) {
    failInterop('invalid invocation');
}
require $autoload;
if (PHP_VERSION !== EXPECTED_PHP_VERSION ||
    PHP_SAPI !== 'cli' ||
    !extension_loaded('mbstring') || !extension_loaded('openssl') || !extension_loaded('sockets') ||
    !defined('STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT') ||
    InstalledVersions::getPrettyVersion('php-amqplib/php-amqplib') !== EXPECTED_AMQPLIB_VERSION ||
    InstalledVersions::getReference('php-amqplib/php-amqplib') !== EXPECTED_AMQPLIB_REFERENCE) {
    failInterop('runtime pin mismatch');
}

$corpus = readJsonObject($corpusPath, MAXIMUM_CORPUS_BYTES);
$configuration = readJsonObject($configurationPath, MAXIMUM_CONFIGURATION_BYTES);
$message = corpusMessage($corpus);
assertCorpusMessage($message, $corpus);
if ($mode === 'self-test') {
    fwrite(STDOUT, 'PHP_SELF_TEST_OK' . PHP_EOL);
    exit(0);
}

$connection = createConnection($configuration);
$channel = null;
try {
    $channel = $connection->channel();
    if ($mode === 'publish') {
        publishConfirmed($channel, $message, $configuration, false);
        fwrite(STDOUT, 'PHP_PUBLISH_OK' . PHP_EOL);
    } elseif ($mode === 'return') {
        publishConfirmed($channel, $message, $configuration, true);
        fwrite(STDOUT, 'PHP_RETURN_OK' . PHP_EOL);
    } else {
        consumeAndValidate($channel, $corpus, $configuration);
        fwrite(STDOUT, 'PHP_CONSUME_OK' . PHP_EOL);
    }
} finally {
    try {
        if ($channel instanceof AMQPChannel) {
            $channel->close();
        }
    } finally {
        $connection->close();
    }
}

function failInterop(string $reason): never
{
    fwrite(STDERR, 'PHP_INTEROP_FAILED ' . $reason . PHP_EOL);
    exit(1);
}

/** @return array<string, mixed> */
function readJsonObject(string $filename, int $maximumBytes): array
{
    $size = filesize($filename);
    if (!is_int($size) || $size < 1 || $size > $maximumBytes) {
        failInterop('invalid JSON input size');
    }
    $contents = file_get_contents($filename);
    if (!is_string($contents) || strlen($contents) !== $size) {
        failInterop('unreadable JSON input');
    }
    $decoded = json_decode($contents, true, 32, JSON_THROW_ON_ERROR);
    if (!is_array($decoded) || array_is_list($decoded)) {
        failInterop('JSON input must be an object');
    }
    return $decoded;
}

/** @param array<string, mixed> $corpus */
function corpusMessage(array $corpus): AMQPMessage
{
    if (($corpus['schema_version'] ?? null) !== 1 ||
        !is_array($corpus['properties'] ?? null) ||
        !is_array($corpus['headers'] ?? null) ||
        !is_string($corpus['body_base64'] ?? null)) {
        failInterop('invalid corpus structure');
    }
    $properties = $corpus['properties'];
    $headers = new AMQPTable();
    foreach ($corpus['headers'] as $header) {
        if (!is_array($header) || !is_string($header['key'] ?? null) || !is_string($header['type'] ?? null)) {
            failInterop('invalid corpus header');
        }
        $key = $header['key'];
        if ($key === PUBLISH_TOKEN_HEADER) {
            failInterop('reserved corpus header');
        }
        switch ($header['type']) {
            case 'string':
                $headers->set($key, requiredString($header, 'string'), AMQPTable::T_STRING_LONG);
                break;
            case 'bool':
                if (!is_bool($header['bool'] ?? null)) {
                    failInterop('invalid bool header');
                }
                $headers->set($key, $header['bool'], AMQPTable::T_BOOL);
                break;
            case 'int64':
                if (!is_int($header['int64'] ?? null)) {
                    failInterop('invalid int64 header');
                }
                $headers->set($key, $header['int64'], AMQPTable::T_INT_LONGLONG);
                break;
            case 'bytes':
                $headers->set($key, decodeBase64(requiredString($header, 'base64')), AMQPTable::T_BYTES);
                break;
            default:
                failInterop('unsupported corpus header');
        }
    }
    $timestamp = strtotime(requiredString($properties, 'timestamp'));
    if (!is_int($timestamp)) {
        failInterop('invalid timestamp');
    }
    if (($properties['delivery_mode'] ?? null) !== 'persistent' ||
        !is_int($properties['expiration_ms'] ?? null) || $properties['expiration_ms'] < 0 ||
        !is_int($properties['priority'] ?? null) || $properties['priority'] < 0 || $properties['priority'] > 255) {
        failInterop('invalid corpus properties');
    }
    return new AMQPMessage(decodeBase64($corpus['body_base64']), [
        'application_headers' => $headers,
        'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        'content_type' => requiredString($properties, 'content_type'),
        'content_encoding' => requiredString($properties, 'content_encoding'),
        'message_id' => requiredString($properties, 'message_id'),
        'correlation_id' => requiredString($properties, 'correlation_id'),
        'reply_to' => requiredString($properties, 'reply_to'),
        'timestamp' => $timestamp,
        'type' => requiredString($properties, 'type'),
        'app_id' => requiredString($properties, 'app_id'),
        'expiration' => (string) $properties['expiration_ms'],
        'priority' => $properties['priority'],
    ]);
}

/** @param array<string, mixed> $corpus */
function assertCorpusMessage(AMQPMessage $message, array $corpus, bool $expectGoPublishToken = false): void
{
    $properties = $corpus['properties'];
    $expected = [
        'delivery_mode' => 2,
        'content_type' => requiredString($properties, 'content_type'),
        'content_encoding' => requiredString($properties, 'content_encoding'),
        'message_id' => requiredString($properties, 'message_id'),
        'correlation_id' => requiredString($properties, 'correlation_id'),
        'reply_to' => requiredString($properties, 'reply_to'),
        'timestamp' => strtotime(requiredString($properties, 'timestamp')),
        'type' => requiredString($properties, 'type'),
        'app_id' => requiredString($properties, 'app_id'),
        'expiration' => (string) $properties['expiration_ms'],
        'priority' => $properties['priority'],
    ];
    foreach ($expected as $key => $value) {
        if (!$message->has($key) || $message->get($key) !== $value) {
            failInterop('corpus property mismatch');
        }
    }
    if (!hash_equals(decodeBase64($corpus['body_base64']), $message->getBody())) {
        failInterop('corpus body mismatch');
    }
    $table = $message->get('application_headers');
    if (!$table instanceof AMQPTable) {
        failInterop('missing application headers');
    }
    $native = $table->getNativeData();
    foreach ($corpus['headers'] as $header) {
        $expectedValue = match ($header['type']) {
            'string' => requiredString($header, 'string'),
            'bool' => $header['bool'],
            'int64' => $header['int64'],
            'bytes' => decodeBase64(requiredString($header, 'base64')),
            default => failInterop('unsupported corpus header'),
        };
        if (!array_key_exists($header['key'], $native) || $native[$header['key']] !== $expectedValue) {
            failInterop('corpus header mismatch');
        }
    }
    if ($expectGoPublishToken) {
        $publishToken = $native[PUBLISH_TOKEN_HEADER] ?? null;
        if (count($native) !== count($corpus['headers']) + 1 ||
            !is_string($publishToken) || $publishToken === '' || strlen($publishToken) > MAXIMUM_PUBLISH_TOKEN_BYTES) {
            failInterop('unexpected Go publication headers');
        }
    } elseif (count($native) !== count($corpus['headers']) || array_key_exists(PUBLISH_TOKEN_HEADER, $native)) {
        failInterop('unexpected corpus headers');
    }
}

/** @param array<string, mixed> $configuration */
function createConnection(array $configuration): AbstractConnection
{
    $endpoints = $configuration['endpoints'] ?? null;
    $tls = $configuration['tls'] ?? null;
    if (!is_array($endpoints) || count($endpoints) !== 1 || !is_array($endpoints[0]) || !is_array($tls)) {
        failInterop('invalid connection configuration');
    }
    $endpoint = $endpoints[0];
    $host = requiredString($endpoint, 'host');
    $port = $endpoint['port'] ?? null;
    if (!is_int($port) || $port < 1 || $port > 65535) {
        failInterop('invalid connection endpoint');
    }
    $serverName = requiredString($tls, 'server_name');
    $ssl = [
        'verify_peer' => true,
        'verify_peer_name' => true,
        'peer_name' => $serverName,
        'crypto_method' => STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT | STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT,
    ];
    optionalFile($tls, 'root_ca_file', $ssl, 'cafile');
    optionalFile($tls, 'client_certificate_file', $ssl, 'local_cert');
    optionalFile($tls, 'client_private_key_file', $ssl, 'local_pk');
    if (isset($ssl['local_cert']) !== isset($ssl['local_pk'])) {
        failInterop('incomplete mTLS identity');
    }
    $config = new AMQPConnectionConfig();
    $config->setIoType(AMQPConnectionConfig::IO_TYPE_STREAM);
    $config->setHost($host);
    $config->setPort($port);
    $config->setUser(requiredString($configuration, 'username'));
    $config->setPassword(requiredString($configuration, 'password'));
    $config->setVhost(requiredString($configuration, 'virtual_host'));
    $config->setIsSecure(true);
    $config->setSslVerify(true);
    $config->setSslVerifyName(true);
    $config->setConnectionTimeout(10.0);
    $config->setReadTimeout(OPERATION_TIMEOUT_SECONDS);
    $config->setWriteTimeout(OPERATION_TIMEOUT_SECONDS);
    $config->setChannelRPCTimeout(OPERATION_TIMEOUT_SECONDS);
    $config->setHeartbeat(10);
    $config->setStreamContext(stream_context_create(['ssl' => $ssl]));
    return AMQPConnectionFactory::create($config);
}

/** @param array<string, mixed> $configuration */
function publishConfirmed(AMQPChannel $channel, AMQPMessage $message, array $configuration, bool $expectReturn): void
{
    $interop = requiredObject($configuration, 'php_interoperability');
    $exchange = requiredString($configuration, 'exchange');
    $route = requiredString($interop, $expectReturn ? 'unroutable_routing_key' : 'php_to_go_routing_key');
    $returned = null;
    $acknowledged = false;
    $rejected = false;
    $channel->set_return_listener(static function (
        int $replyCode,
        string $replyText,
        string $returnedExchange,
        string $routingKey,
        AMQPMessage $returnedMessage
    ) use (&$returned): void {
        $returned = [$replyCode, $returnedExchange, $routingKey, $returnedMessage->get('message_id')];
    });
    $channel->set_ack_handler(static function (AMQPMessage $confirmedMessage) use (&$acknowledged, $message): void {
        if ($confirmedMessage->get('message_id') !== $message->get('message_id')) {
            failInterop('publisher confirmation mismatch');
        }
        $acknowledged = true;
    });
    $channel->set_nack_handler(static function (AMQPMessage $rejectedMessage) use (&$rejected, $message): void {
        if ($rejectedMessage->get('message_id') !== $message->get('message_id')) {
            failInterop('publisher rejection mismatch');
        }
        $rejected = true;
    });
    $channel->confirm_select();
    $channel->basic_publish($message, $exchange, $route, true, false);
    $channel->wait_for_pending_acks_returns(OPERATION_TIMEOUT_SECONDS);
    if (!$acknowledged || $rejected) {
        failInterop('publication was not positively confirmed');
    }
    if ($expectReturn) {
        if ($returned !== [312, $exchange, $route, $message->get('message_id')]) {
            failInterop('mandatory return mismatch');
        }
    } elseif ($returned !== null) {
        failInterop('routable publication was returned');
    }
}

/**
 * @param array<string, mixed> $corpus
 * @param array<string, mixed> $configuration
 */
function consumeAndValidate(AMQPChannel $channel, array $corpus, array $configuration): void
{
    $interop = requiredObject($configuration, 'php_interoperability');
    $queue = requiredString($interop, 'go_to_php_queue');
    $deadline = microtime(true) + OPERATION_TIMEOUT_SECONDS;
    do {
        $message = $channel->basic_get($queue, false);
        if ($message instanceof AMQPMessage) {
            assertCorpusMessage($message, $corpus, true);
            if ($message->get('exchange') !== requiredString($configuration, 'exchange') ||
                $message->get('routing_key') !== requiredString($interop, 'go_to_php_routing_key')) {
                failInterop('delivery route mismatch');
            }
            $channel->basic_ack($message->getDeliveryTag(), false);
            return;
        }
        usleep(50000);
    } while (microtime(true) < $deadline);
    failInterop('delivery timeout');
}

/** @param array<string, mixed> $values */
function requiredString(array $values, string $key): string
{
    $value = $values[$key] ?? null;
    if (!is_string($value) || $value === '' || strlen($value) > 4096) {
        failInterop('missing bounded string');
    }
    return $value;
}

/** @param array<string, mixed> $values @return array<string, mixed> */
function requiredObject(array $values, string $key): array
{
    $value = $values[$key] ?? null;
    if (!is_array($value) || array_is_list($value)) {
        failInterop('missing object');
    }
    return $value;
}

function decodeBase64(string $value): string
{
    $decoded = base64_decode($value, true);
    if (!is_string($decoded)) {
        failInterop('invalid base64');
    }
    return $decoded;
}

/**
 * @param array<string, mixed> $source
 * @param array<string, bool|int|string> $target
 */
function optionalFile(array $source, string $sourceKey, array &$target, string $targetKey): void
{
    $filename = $source[$sourceKey] ?? '';
    if (!is_string($filename)) {
        failInterop('invalid TLS file');
    }
    if ($filename !== '') {
        if (!is_file($filename)) {
            failInterop('missing TLS file');
        }
        $target[$targetKey] = $filename;
    }
}
