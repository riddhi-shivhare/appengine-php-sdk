<?php
/**
 * Copyright 2021 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * The PushQueue class, which is part of the Task Queue API.
 *
 */

namespace Google\AppEngine\Api\TaskQueue;

use Google\AppEngine\Runtime\ApiProxy;
use Google\AppEngine\Runtime\ApplicationError;
use google\appengine\TaskQueueAddRequest;
use google\appengine\TaskQueueAddRequest\RequestMethod;
use google\appengine\TaskQueueAddResponse;
use google\appengine\TaskQueueBulkAddRequest;
use google\appengine\TaskQueueBulkAddResponse;
use google\appengine\TaskQueueServiceError\ErrorCode;

/**
 * A PushQueue executes PushTasks by sending the task back to the application
 * in the form of an HTTP request to one of the application's handlers.
 */
final class PushQueue {
  /**
   * The maximum number of tasks in a single call addTasks.
   */
  const MAX_TASKS_PER_ADD = 100;

  const GAE_PUSHQUEUE_BACKEND = 'GAE_PUSHQUEUE_BACKEND';
  const CLOUD_TASK_BACKEND = 'CLOUD_TASK';

  private $name;

  // The Guzzle Client used to make http requests.
  private $guzzle_client = null;

  private static $methods = [
    'POST' => RequestMethod::POST,
    'GET' => RequestMethod::GET,
    'HEAD' => RequestMethod::HEAD,
    'PUT' => RequestMethod::PUT,
    'DELETE' => RequestMethod::DELETE
  ];

  /**
   * Construct a PushQueue
   *
   * @param string $name The name of the queue.
   * @param \GuzzleHttp\Client $mock_client Mocked Guzzle Client used to make http requests.
   */
  public function __construct($name = 'default', $mock_client = null) {
    if (!is_string($name)) {
      throw new \InvalidArgumentException(
          '$name must be a string. Actual type: ' . gettype($name));
    }
    # TODO: validate queue name length and regex.
    $this->name = $name;

    if (isset($mock_client)) {
      $this->guzzle_client = $mock_client;
    } else {
      $this->guzzle_client = new \GuzzleHttp\Client();
    }
  }

  /**
   * Return the queue's name.
   *
   * @return string The queue's name.
   */
  public function getName() {
    return $this->name;
  }

  private static function errorCodeToException($error) {
    switch($error) {
      case ErrorCode::UNKNOWN_QUEUE:
        return new TaskQueueException('Unknown queue');
      case ErrorCode::TRANSIENT_ERROR:
        return new TransientTaskQueueException(
            'Temporary error, please re-try');
      case ErrorCode::INTERNAL_ERROR:
        return new TaskQueueException('Internal error');
      case ErrorCode::TASK_TOO_LARGE:
        return new TaskQueueException('Task too large');
      case ErrorCode::INVALID_TASK_NAME:
        return new TaskQueueException('Invalid task name');
      case ErrorCode::INVALID_QUEUE_NAME:
      case ErrorCode::TOMBSTONED_QUEUE:
        return new TaskQueueException('Invalid queue name');
      case ErrorCode::INVALID_URL:
        return new TaskQueueException('Invalid URL');
      case ErrorCode::PERMISSION_DENIED:
        return new TaskQueueException('Permission Denied');

      // Both TASK_ALREADY_EXISTS and TOMBSTONED_TASK are translated into the
      // same exception. This is in keeping with the Java API but different to
      // the Python API. Knowing that the task is tombstoned isn't particularly
      // interesting: the main point is that it has already been added.
      case ErrorCode::TASK_ALREADY_EXISTS:
      case ErrorCode::TOMBSTONED_TASK:
        return new TaskAlreadyExistsException(
            'Task with the same name exists already');
      case ErrorCode::INVALID_ETA:
        return new TaskQueueException('Invalid delay_seconds');
      case ErrorCode::INVALID_REQUEST:
        return new TaskQueueException('Invalid request');
      case ErrorCode::DUPLICATE_TASK_NAME:
        return new TaskQueueException(
            'Duplicate task names in addTasks request.');
      case ErrorCode::TOO_MANY_TASKS:
        return new TaskQueueException('Too many tasks in request.');
      case ErrorCode::INVALID_QUEUE_MODE:
        return new TaskQueueException('Cannot add a PushTask to a pull queue.');
      default:
        return new TaskQueueException('Error Code: ' . $error);
    }
  }

  /**
   * Add tasks to the queue.
   *
   * @param PushTask[] $tasks The tasks to be added to the queue.
   *
   * @return An array containing the name of each task added, with the same
   * ordering as $tasks.
   *
   * @throws TaskAlreadyExistsException if a task of the same name already
   * exists in the queue.
   * If this exception is raised, the caller can be guaranteed that all tasks
   * were successfully added either by this call or a previous call. Another way
   * to express it is that, if any task failed to be added for a different
   * reason, a different exception will be thrown.
   * @throws TaskQueueException if there was a problem using the service.
   */
  public function addTasks($tasks) {
    if (!is_array($tasks)) {
      throw new \InvalidArgumentException(
          '$tasks must be an array. Actual type: ' . gettype($tasks));
    }
    if (empty($tasks)) {
      return [];
    }
    if (count($tasks) > self::MAX_TASKS_PER_ADD) {
      throw new \InvalidArgumentException(
          '$tasks must contain at most ' . self::MAX_TASKS_PER_ADD .
          ' tasks. Actual size: ' . count($tasks));
    }

    $backend = getenv(self::GAE_PUSHQUEUE_BACKEND);
    if ($backend === self::CLOUD_TASK_BACKEND) {
      if (count($tasks) > 1) {
        throw new \RuntimeException('Batch operations are not supported for Cloud Tasks backend yet.');
      }
      return [$this->createCloudTask($tasks[0])];
    }

    $req = new TaskQueueBulkAddRequest();
    $resp = new TaskQueueBulkAddResponse();

    $names = [];
    $current_time = microtime(true);
    foreach ($tasks as $task) {
      if (!($task instanceof PushTask)) {
        throw new \InvalidArgumentException(
            'All values in $tasks must be instances of PushTask. ' .
            'Actual type: ' . gettype($task));
      }
      $names[] = $task->getName();
      $add = $req->addAddRequest();
      $add->setQueueName($this->name);
      $add->setTaskName($task->getName());
      $add->setEtaUsec(($current_time + $task->getDelaySeconds()) * 1e6);
      $add->setMethod(self::$methods[$task->getMethod()]);
      $add->setUrl($task->getUrl());
      foreach ($task->getHeaders() as $header) {
        $pair = explode(':', $header, 2);
        $header_pb = $add->addHeader();
        $header_pb->setKey(trim($pair[0]));
        $header_pb->setValue(trim($pair[1]));
      }
      // TODO: Replace getQueryData() with getBody() and simplify the following
      // block.
      if ($task->getMethod() == 'POST' || $task->getMethod() == 'PUT') {
        if ($task->getQueryData()) {
          $add->setBody(http_build_query($task->getQueryData()));
        }
      }
      if ($add->byteSizePartial() > PushTask::MAX_TASK_SIZE_BYTES) {
        throw new TaskQueueException('Task greater than maximum size of ' .
            PushTask::MAX_TASK_SIZE_BYTES . '. size: ' .
            $add->byteSizePartial());
      }
    }

    try {
      ApiProxy::makeSyncCall('taskqueue', 'BulkAdd', $req, $resp);
    } catch (ApplicationError $e) {
      throw self::errorCodeToException($e->getApplicationError());
    }

    // Update $names with any generated task names. Also, check if there are any
    // error responses.
    $results = $resp->getTaskResultList();
    $exception = null;
    foreach ($results as $index => $task_result) {
      if ($task_result->hasChosenTaskName()) {
        $names[$index] = $task_result->getChosenTaskName();
      }
      if ($task_result->getResult() != ErrorCode::OK) {
        $exception = self::errorCodeToException($task_result->getResult());
        // Other exceptions take precedence over TaskAlreadyExistsException.
        if (!($exception instanceof TaskAlreadyExistsException)) {
          throw $exception;
        }
      }
    }
    if (isset($exception)) {
      throw $exception;
    }
    return $names;
  }

  /**
   * Create a task using Cloud Tasks API.
   *
   * @param PushTask $task The task to create.
   * @return string The name of the created task.
   * @throws TaskQueueException if there was a problem using the service.
   */
  private function createCloudTask(PushTask $task) {
    $projectId = \Google\AppEngine\Api\AppIdentity\AppIdentityService::getApplicationId();
    $location = getenv('GAE_LOCATION');
    if (!$location) {
      throw new \RuntimeException('GAE_LOCATION environment variable is not set.');
    }
    $queue = $this->name;
    
    $accessTokenData = \Google\AppEngine\Api\AppIdentity\AppIdentityService::getAccessToken(['https://www.googleapis.com/auth/cloud-platform']);
    $token = $accessTokenData['access_token'];
    
    $url = sprintf('https://cloudtasks.googleapis.com/v2beta2/projects/%s/locations/%s/queues/%s/tasks', $projectId, $location, $queue);
    
    $body = [
      'task' => [
        'appEngineHttpRequest' => [
          'httpMethod' => $task->getMethod(),
          'relativeUri' => $task->getUrl(),
        ]
      ]
    ];
    
    $retryConfig = $task->getRetryConfig();
    if (!empty($retryConfig)) {
      $body['task']['retryConfig'] = [];
      if (isset($retryConfig['max_attempts'])) {
        $body['task']['retryConfig']['maxAttempts'] = $retryConfig['max_attempts'];
      }
      if (isset($retryConfig['min_backoff'])) {
        $body['task']['retryConfig']['minBackoff'] = $retryConfig['min_backoff']['seconds'] . 's';
      }
      if (isset($retryConfig['max_backoff'])) {
        $body['task']['retryConfig']['maxBackoff'] = $retryConfig['max_backoff']['seconds'] . 's';
      }
    }
    
    $headers = [];
    foreach ($task->getHeaders() as $header) {
      $pair = explode(':', $header, 2);
      $headers[trim($pair[0])] = trim($pair[1]);
    }
    
    if (!empty($headers)) {
      $body['task']['appEngineHttpRequest']['headers'] = $headers;
    }
    
    if ($task->getMethod() == 'POST' || $task->getMethod() == 'PUT') {
      if ($task->getQueryData()) {
        $body['task']['appEngineHttpRequest']['body'] = base64_encode(http_build_query($task->getQueryData()));
      }
    }
    
    if ($task->getName()) {
      $body['task']['name'] = sprintf('projects/%s/locations/%s/queues/%s/tasks/%s', $projectId, $location, $queue, $task->getName());
    }
    
    if ($task->getDelaySeconds() > 0) {
      $eta = time() + $task->getDelaySeconds();
      $body['task']['scheduleTime'] = gmdate('Y-m-d\TH:i:s\Z', $eta);
    }
    
    try {
      $response = $this->guzzle_client->post($url, [
        'headers' => [
          'Authorization' => 'Bearer ' . $token,
          'Content-Type' => 'application/json',
        ],
        'json' => $body,
      ]);
      
      $respBody = json_decode($response->getBody(), true);
      $fullName = $respBody['name'];
      $parts = explode('/', $fullName);
      return end($parts);
      
    } catch (\GuzzleHttp\Exception\RequestException $e) {
      if ($e->getResponse() && $e->getResponse()->getStatusCode() == 409) {
        throw new TaskAlreadyExistsException('Task with the same name exists already');
      }
      throw new TaskQueueException('Cloud Tasks API error: ' . $e->getMessage());
    }
  }
}
