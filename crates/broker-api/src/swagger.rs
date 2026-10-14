//! OpenAPI 3.0 specification and Swagger UI handler for IndraMQTT management API.

/// Returns the OpenAPI 3.0.3 specification in JSON format.
pub fn openapi_spec_json() -> &'static str {
    r#"{
  "openapi": "3.0.3",
  "info": {
    "title": "IndraMQTT Management API",
    "description": "Management API of the broker. Use it to read and change broker state. This document lists the /api/v1 management operations and /healthz. The dashboard assets and the /api/v5 surface are separate and are not listed here.",
    "version": "1.0.0",
    "contact": {
      "name": "IndraMQTT Engineering",
      "url": "https://github.com/ankur-paan/indramqtt"
    },
    "license": {
      "name": "MIT OR Apache-2.0"
    }
  },
  "servers": [
    {
      "url": "http://localhost:8081",
      "description": "Local IndraMQTT Node"
    }
  ],
  "tags": [
    { "name": "Cluster & Nodes", "description": "Cluster topology, node health, and run-queues" },
    { "name": "Clients & Sessions", "description": "Connected MQTT 3.1.1 / 5.0 client sessions" },
    { "name": "Observability", "description": "Live throughput, latency, and Prometheus metrics" },
    { "name": "Rule Engine", "description": "Streaming SQL rules and analytical functions" },
    { "name": "Data Connectors", "description": "External data sinks (Kafka, Postgres, Redis, MySQL, S3)" },
    { "name": "Authentication & ACL", "description": "User credential management and topic ACL rules" },
    { "name": "Configuration", "description": "Configuration validation, reload, history and setting lookup" },
    { "name": "Licence", "description": "Commercial licence request, install and status" }
  ],
  "paths": {
    "/healthz": {
      "get": {
        "tags": ["Cluster & Nodes"],
        "summary": "Node health check",
        "operationId": "healthCheck",
        "responses": {
          "200": {
            "description": "Node is healthy and operating normally",
            "content": {
              "text/plain": {
                "schema": { "type": "string", "example": "OK" }
              }
            }
          }
        }
      }
    },
    "/api/v1/nodes": {
      "get": {
        "tags": ["Cluster & Nodes"],
        "summary": "Get cluster node status and metrics",
        "operationId": "getNodes",
        "responses": {
          "200": {
            "description": "Node status with live counts",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "node_id": { "type": "string" },
                    "status": { "type": "string" },
                    "version": { "type": "string" },
                    "connections": { "type": "integer" },
                    "rules": { "type": "integer" }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/metrics": {
      "get": {
        "tags": ["Observability"],
        "summary": "Get broker metrics",
        "operationId": "getMetrics",
        "responses": {
          "200": {
            "description": "Prometheus text exposition of live counters",
            "content": {
              "text/plain": {
                "schema": { "type": "string" }
              }
            }
          }
        }
      }
    },
    "/api/v1/clients": {
      "get": {
        "tags": ["Clients & Sessions"],
        "summary": "List active client sessions",
        "operationId": "listClients",
        "responses": {
          "200": {
            "description": "Array of connected client identifiers",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": { "type": "string" }
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/clients/{id}": {
      "get": {
        "tags": ["Clients & Sessions"],
        "summary": "Get detailed client session information",
        "operationId": "getClientDetail",
        "parameters": [
          {
            "name": "id",
            "in": "path",
            "required": true,
            "description": "Client ID",
            "schema": { "type": "string" }
          }
        ],
        "responses": {
          "200": {
            "description": "Client session details",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "client_id": { "type": "string" },
                    "session_id": { "type": "integer" },
                    "conn_id": { "type": "integer" },
                    "keepalive_secs": { "type": "integer" },
                    "clean_start": { "type": "boolean" },
                    "connected": { "type": "boolean" },
                    "queued": { "type": "integer" },
                    "subscriptions": {
                      "type": "array",
                      "items": { "type": "string" }
                    }
                  }
                }
              }
            }
          },
          "404": { "description": "Client not found" }
        }
      }
    },
    "/api/v1/auth/users": {
      "get": {
        "tags": ["Authentication & ACL"],
        "summary": "List all configured users and quotas",
        "operationId": "listUsers",
        "parameters": [
          {
            "name": "tenant",
            "in": "query",
            "description": "Tenant table to list; absent lists the default tenant",
            "schema": { "type": "string" }
          }
        ],
        "responses": {
          "200": {
            "description": "List of users with quota allocations",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "username": { "type": "string" },
                      "quotas": {
                        "type": "object",
                        "properties": {
                          "max_connections": { "type": "integer" },
                          "max_publish_rate": { "type": "integer" },
                          "max_publish_burst": { "type": "integer" }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      },
      "post": {
        "tags": ["Authentication & ACL"],
        "summary": "Create or update user credentials and quotas",
        "operationId": "createUser",
        "parameters": [
          {
            "name": "tenant",
            "in": "query",
            "description": "Tenant table to write; absent writes the default tenant",
            "schema": { "type": "string" }
          }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["username", "password"],
                "properties": {
                  "username": { "type": "string", "example": "doc-user-1" },
                  "password": { "type": "string", "example": "Secret123!" },
                  "quotas": {
                    "type": "object",
                    "properties": {
                      "max_connections": { "type": "integer" },
                      "max_publish_rate": { "type": "integer" },
                      "max_publish_burst": { "type": "integer" }
                    }
                  }
                }
              }
            }
          }
        },
        "responses": {
          "201": { "description": "User created or updated successfully" },
          "400": { "description": "Invalid input payload" },
          "500": { "description": "Registry save failed after the in-memory write" }
        }
      }
    },
    "/api/v1/auth/acls": {
      "get": {
        "tags": ["Authentication & ACL"],
        "summary": "List ordered ACL rules",
        "operationId": "listAcls",
        "parameters": [
          {
            "name": "tenant",
            "in": "query",
            "description": "Tenant table to list; absent lists the default tenant",
            "schema": { "type": "string" }
          }
        ],
        "responses": {
          "200": {
            "description": "List of ACL rules evaluated in order",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "client_pattern": { "type": "string" },
                      "action": { "type": "string", "enum": ["publish", "subscribe", "all"] },
                      "topic_pattern": { "type": "string" },
                      "allow": { "type": "boolean" }
                    }
                  }
                }
              }
            }
          }
        }
      },
      "post": {
        "tags": ["Authentication & ACL"],
        "summary": "Add new ACL rule",
        "operationId": "createAcl",
        "parameters": [
          {
            "name": "tenant",
            "in": "query",
            "description": "Tenant table to write; absent writes the default tenant",
            "schema": { "type": "string" }
          }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["client_pattern", "action", "topic_pattern", "allow"],
                "properties": {
                  "client_pattern": { "type": "string", "example": "doc-client" },
                  "action": { "type": "string", "enum": ["publish", "subscribe", "all"] },
                  "topic_pattern": { "type": "string", "example": "sensors/#" },
                  "allow": { "type": "boolean" }
                }
              }
            }
          }
        },
        "responses": {
          "201": { "description": "ACL rule registered" },
          "400": { "description": "Invalid ACL input" },
          "500": { "description": "Registry save failed after the in-memory write" }
        }
      }
    },
    "/api/v1/auth/users/{username}": {
      "delete": {
        "tags": ["Authentication & ACL"],
        "summary": "Delete one user",
        "operationId": "deleteUser",
        "parameters": [
          {
            "name": "username",
            "in": "path",
            "required": true,
            "description": "User name",
            "schema": { "type": "string" }
          },
          {
            "name": "tenant",
            "in": "query",
            "description": "Tenant table to write; absent writes the default tenant",
            "schema": { "type": "string" }
          }
        ],
        "responses": {
          "204": { "description": "User deleted" },
          "404": { "description": "User not found" },
          "500": { "description": "Registry save failed after the in-memory write" }
        }
      }
    },
    "/api/v1/auth/verify_stats": {
      "get": {
        "tags": ["Authentication & ACL"],
        "summary": "Show password verification counters",
        "operationId": "getVerifyStats",
        "responses": {
          "200": {
            "description": "Verification and deduplication counters",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "verifications": { "type": "integer" },
                    "dedup_joins": { "type": "integer" }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/auth/acls/{id}": {
      "delete": {
        "tags": ["Authentication & ACL"],
        "summary": "Delete one ACL rule by index",
        "operationId": "deleteAcl",
        "parameters": [
          {
            "name": "id",
            "in": "path",
            "required": true,
            "description": "Index in the tenant rule list",
            "schema": { "type": "integer" }
          },
          {
            "name": "tenant",
            "in": "query",
            "description": "Tenant table to write; absent writes the default tenant",
            "schema": { "type": "string" }
          }
        ],
        "responses": {
          "204": { "description": "ACL rule deleted" },
          "404": { "description": "ACL rule not found" },
          "500": { "description": "Registry save failed after the in-memory write" }
        }
      }
    },
    "/api/v1/rules": {
      "get": {
        "tags": ["Rule Engine"],
        "summary": "List all streaming SQL rules",
        "operationId": "listRules",
        "responses": {
          "200": {
            "description": "Array of configured rules",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "id": { "type": "string" },
                      "name": { "type": "string" },
                      "tenant": { "type": "string" },
                      "topic_filter": { "type": "string" },
                      "sql_query": { "type": "string" },
                      "enabled": { "type": "boolean" }
                    }
                  }
                }
              }
            }
          }
        }
      },
      "post": {
        "tags": ["Rule Engine"],
        "summary": "Create new streaming SQL rule",
        "operationId": "createRule",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["name", "topic_filter"],
                "properties": {
                  "name": { "type": "string", "example": "doc-rule-1" },
                  "tenant": { "type": "string" },
                  "topic_filter": { "type": "string", "example": "sensors/#" },
                  "sql_query": { "type": "string" },
                  "actions": {
                    "type": "array",
                    "items": {
                      "type": "object",
                      "properties": {
                        "type": { "type": "string", "enum": ["republish", "log", "forwardconnector"] },
                        "topic": { "type": "string" },
                        "qos": { "type": "integer" },
                        "connector_id": { "type": "string" }
                      }
                    }
                  },
                  "enabled": { "type": "boolean", "default": true }
                }
              }
            }
          }
        },
        "responses": {
          "201": { "description": "Rule created" },
          "400": { "description": "Invalid rule" },
          "500": { "description": "Rule save failed after the in-memory write" }
        }
      }
    },
    "/api/v1/rules/test": {
      "post": {
        "tags": ["Rule Engine"],
        "summary": "Test streaming SQL query against mock payload",
        "operationId": "testRule",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["sql_query", "topic", "payload"],
                "properties": {
                  "sql_query": { "type": "string", "example": "SELECT * FROM \"sensors/#\"" },
                  "topic": { "type": "string", "example": "sensors/1" },
                  "payload": { "type": "string", "example": "{\"temp\": 21}" }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Rule test evaluation result",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "matched": { "type": "boolean" },
                    "projected": { }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/rules/functions": {
      "get": {
        "tags": ["Rule Engine"],
        "summary": "List catalog of analytical SQL functions",
        "operationId": "listFunctions",
        "responses": {
          "200": {
          "description": "Catalog of supported streaming functions",
              "content": {
                "application/json": {
                  "schema": {
                    "type": "array",
                    "items": {
                      "type": "object",
                      "properties": {
                        "name": { "type": "string" },
                        "category": { "type": "string" },
                        "description": { "type": "string" },
                        "aggregate": { "type": "boolean" },
                        "arity": { "type": "string" },
                        "example": { "type": "string" }
                      }
                    }
                  }
                }
              }
          }
        }
      }
    },
    "/api/v1/rules/sources": {
      "get": {
        "tags": ["Rule Engine"],
        "summary": "List available rule event sources",
        "operationId": "listRuleSources",
        "responses": {
          "200": {
            "description": "List of event sources",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": { "type": "object" }
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/rules/{id}": {
      "get": {
        "tags": ["Rule Engine"],
        "summary": "Get rule by ID",
        "operationId": "getRule",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Rule details" },
          "404": { "description": "Rule not found" }
        }
      },
      "delete": {
        "tags": ["Rule Engine"],
        "summary": "Delete rule by ID",
        "operationId": "deleteRule",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "204": { "description": "Rule deleted" },
          "404": { "description": "Rule not found" },
          "500": { "description": "Rule save failed after the in-memory write" }
        }
      }
    },
    "/api/v1/connectors": {
      "get": {
        "tags": ["Data Connectors"],
        "summary": "List registered data connectors",
        "operationId": "listConnectors",
        "responses": {
          "200": {
            "description": "List of active connectors",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "id": { "type": "string" },
                      "kind": { "type": "string" },
                      "tier": { "type": "string" }
                    }
                  }
                }
              }
            }
          }
        }
      },
      "post": {
        "tags": ["Data Connectors"],
        "summary": "Register new data connector",
        "operationId": "createConnector",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["id", "kind"],
                "properties": {
                  "id": { "type": "string", "example": "doc-conn-1" },
                  "kind": { "type": "string", "example": "logger" },
                  "config": { "type": "object" }
                }
              }
            }
          }
        },
        "responses": {
          "201": { "description": "Connector registered" },
          "400": { "description": "Invalid connector" }
        }
      }
    },
    "/api/v1/config/validate": {
      "post": {
        "tags": ["Configuration"],
        "summary": "Validate a candidate configuration snapshot",
        "operationId": "validateConfig",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["snapshot"],
                "properties": {
                  "snapshot": { "type": "object" }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Candidate applies cleanly",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "ok": { "type": "boolean", "example": true }
                  }
                }
              }
            }
          },
          "400": { "description": "Candidate names each offending setting" }
        }
      }
    },
    "/api/v1/config/diff": {
      "post": {
        "tags": ["Configuration"],
        "summary": "Show the diff a reload would apply",
        "operationId": "diffConfig",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["snapshot"],
                "properties": {
                  "snapshot": { "type": "object" }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Setting diff with redacted secrets",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "diff": {
                      "type": "array",
                      "items": { "type": "object" }
                    }
                  }
                }
              }
            }
          },
          "400": { "description": "Candidate names each offending setting" }
        }
      }
    },
    "/api/v1/config/reload": {
      "post": {
        "tags": ["Configuration"],
        "summary": "Apply a candidate configuration snapshot",
        "operationId": "reloadConfig",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["snapshot"],
                "properties": {
                  "snapshot": { "type": "object" },
                  "actor": { "type": "string" },
                  "summary": { "type": "string" }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Reloaded version with applied diff",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "version": { "type": "integer" },
                    "diff": {
                      "type": "array",
                      "items": { "type": "object" }
                    },
                    "pending_live": {
                      "type": "array",
                      "items": { "type": "string" }
                    }
                  }
                }
              }
            }
          },
          "400": { "description": "Candidate names each offending setting" },
          "500": { "description": "Reload failed after validation" }
        }
      }
    },
    "/api/v1/config/history": {
      "get": {
        "tags": ["Configuration"],
        "summary": "List configuration versions newest first",
        "operationId": "listConfigHistory",
        "responses": {
          "200": {
            "description": "Version list without snapshots",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "versions": {
                      "type": "array",
                      "items": { "type": "object" }
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/config/history/{id}": {
      "get": {
        "tags": ["Configuration"],
        "summary": "Get one configuration version",
        "operationId": "getConfigHistoryVersion",
        "parameters": [
          {
            "name": "id",
            "in": "path",
            "required": true,
            "description": "Version id",
            "schema": { "type": "integer" }
          }
        ],
        "responses": {
          "200": {
            "description": "Version with redacted snapshot",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "id": { "type": "integer" },
                    "actor": { "type": "string" },
                    "timestamp_secs": { "type": "integer" },
                    "summary": { "type": "string" },
                    "changes": {
                      "type": "array",
                      "items": { "type": "string" }
                    },
                    "snapshot": { "type": "object" }
                  }
                }
              }
            }
          },
          "404": { "description": "Version does not exist" }
        }
      }
    },
    "/api/v1/config/history/{id}/restore": {
      "post": {
        "tags": ["Configuration"],
        "summary": "Restore one configuration version",
        "operationId": "restoreConfigHistoryVersion",
        "parameters": [
          {
            "name": "id",
            "in": "path",
            "required": true,
            "description": "Version id",
            "schema": { "type": "integer" }
          }
        ],
        "requestBody": {
          "required": false,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "actor": { "type": "string" }
                }
              }
            }
          }
        },
        "responses": {
          "200": { "description": "Restored version with applied diff" },
          "404": { "description": "Version does not exist" }
        }
      }
    },
    "/api/v1/config/explain": {
      "get": {
        "tags": ["Configuration"],
        "summary": "Explain one setting value and its source layer",
        "operationId": "explainConfig",
        "parameters": [
          {
            "name": "key",
            "in": "query",
            "required": true,
            "description": "Setting key such as session.max_qos0_backlog",
            "schema": { "type": "string", "example": "session.max_qos0_backlog" }
          }
        ],
        "responses": {
          "200": {
            "description": "Effective value with source layer",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object"
                }
              }
            }
          },
          "400": { "description": "Query key is missing" },
          "404": { "description": "Unknown setting key" }
        }
      }
    },
    "/api/v1/licence/request": {
      "get": {
        "tags": ["Licence"],
        "summary": "Show the installation licence request",
        "operationId": "getLicenceRequest",
        "responses": {
          "200": {
            "description": "Licence request safe to send",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object"
                }
              }
            }
          },
          "503": { "description": "Request cannot be generated" }
        }
      }
    },
    "/api/v1/licence/install": {
      "post": {
        "tags": ["Licence"],
        "summary": "Install a licence token",
        "operationId": "installLicence",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["token"],
                "properties": {
                  "token": { "type": "string", "example": "INDRA-ENT-V2.doc-payload.doc-signature" }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Installed licence payload",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "customer": { "type": "string" },
                    "expires_at": { "type": "integer" },
                    "max_nodes": { "type": "integer" },
                    "kid": { "type": "string" }
                  }
                }
              }
            }
          },
          "400": { "description": "Token rejected" },
          "409": { "description": "Token names another installation" }
        }
      }
    },
    "/api/v1/licence/status": {
      "get": {
        "tags": ["Licence"],
        "summary": "Show licence status",
        "operationId": "getLicenceStatus",
        "responses": {
          "200": {
            "description": "Identity, customer, expiry and entitlements",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object"
                }
              }
            }
          }
        }
      }
    }
  }
}"#
}

/// Standalone Swagger UI HTML that points to `/api-docs/openapi.json`.
pub fn swagger_ui_html() -> &'static str {
    r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>IndraMQTT - Interactive Swagger API Explorer</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
  <style>
    body {
      margin: 0;
      background: #0d1117;
      color: #e6edf3;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 12px 24px;
      background: #161b22;
      border-bottom: 1px solid #30363d;
    }
    .topbar h1 {
      margin: 0;
      font-size: 18px;
      font-weight: 600;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .topbar a {
      color: #58a6ff;
      text-decoration: none;
      font-size: 14px;
    }
    .topbar a:hover {
      text-decoration: underline;
    }
    .swagger-ui {
      filter: invert(88%) hue-rotate(180deg);
    }
    .swagger-ui .topbar { display: none !important; }
  </style>
</head>
<body>
  <div class="topbar">
    <h1>
      <span>⚡</span> IndraMQTT OpenAPI 3.0 Interactive Explorer
    </h1>
    <div>
      <a href="/dashboard">← Back to Management Dashboard</a>
    </div>
  </div>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = () => {
      window.ui = SwaggerUIBundle({
        url: '/api-docs/openapi.json',
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIBundle.SwaggerUIStandalonePreset
        ],
        layout: "BaseLayout"
      });
    };
  </script>
</body>
</html>"#
}

#[cfg(test)]
mod docs_reference_drift_tests {
    //! Drift test for the generated API pages (DOC-02).
    //!
    //! The generator owns the pages. This test runs it in check mode.
    //! It then checks each page against the served document in both
    //! directions, so no operation is missing and none is invented.
    //! It then checks the served document against the router in
    //! `lib.rs` in both directions over `/api/v1` and `/healthz`:
    //! each served operation names one `.route` literal and method,
    //! and each router route names one served operation. Only exact
    //! literal matches count, never substring matches.

    use std::path::PathBuf;

    use super::openapi_spec_json;

    /// Locates the workspace root from this crate directory.
    fn workspace_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
    }

    /// Runs the generator in check mode for one artefact.
    fn generator_check(artefact: &str) -> std::process::Output {
        std::process::Command::new("python3")
            .arg("tools/gen_reference_docs.py")
            .arg("--check")
            .arg(artefact)
            .current_dir(workspace_root())
            .output()
            .expect("python3 runs the generator")
    }

    /// Runs the generator against one served OpenAPI JSON file.
    fn generator_check_served(artefact: &str, served: &std::path::Path) -> std::process::Output {
        std::process::Command::new("python3")
            .arg("tools/gen_reference_docs.py")
            .arg("--check")
            .arg(artefact)
            .arg("--openapi-json")
            .arg(served)
            .current_dir(workspace_root())
            .output()
            .expect("python3 runs the generator")
    }

    /// Reads `.route("literal", ...)` calls with their methods.
    ///
    /// Each entry holds the literal and its method set. The scan
    /// tracks paren depth, so nested calls stay inside one route.
    /// The literal may sit on the next line. Only exact literal
    /// matches count, never substring matches.
    fn router_routes(src: &str) -> Vec<(String, Vec<String>)> {
        const METHODS: [&str; 6] = ["get", "post", "put", "delete", "patch", "head"];
        let mut routes = Vec::new();
        let bytes = src.as_bytes();
        let mut index = 0;
        while index < bytes.len() {
            let rest = &src[index..];
            let call = match rest.find(".route(") {
                Some(found) => index + found + 7,
                None => break,
            };
            // Skip whitespace. The literal may sit on the next line.
            let mut cursor = call;
            while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            if cursor >= bytes.len() || bytes[cursor] != b'"' {
                index = cursor + 1;
                continue;
            }
            let start = cursor + 1;
            let end = match src[start..].find('"') {
                Some(found) => start + found,
                None => break,
            };
            let literal = src[start..end].to_string();
            // The `.route(` paren is already open. Scan to its
            // close. The body holds each method call.
            let mut depth = 1usize;
            let mut cursor = end + 1;
            let body_start = cursor;
            while cursor < bytes.len() && depth > 0 {
                match bytes[cursor] {
                    b'(' => depth += 1,
                    b')' => depth -= 1,
                    _ => {}
                }
                cursor += 1;
            }
            let body = &src[body_start..cursor.saturating_sub(1)];
            let mut methods = Vec::new();
            for method in METHODS {
                let want = format!("{method}(");
                let mut at = 0;
                while let Some(found) = body[at..].find(&want) {
                    let pos = at + found;
                    let prev = if pos == 0 {
                        None
                    } else {
                        Some(body.as_bytes()[pos - 1])
                    };
                    let boundary =
                        prev.is_none_or(|byte| !byte.is_ascii_alphanumeric() && byte != b'_');
                    if boundary && !methods.contains(&method.to_string()) {
                        methods.push(method.to_string());
                    }
                    at = pos + want.len();
                }
            }
            routes.push((literal, methods));
            index = cursor;
        }
        routes
    }

    /// Maps one Axum literal to OpenAPI path form.
    ///
    /// `/api/v1/rules/:id` becomes `/api/v1/rules/{id}`.
    fn openapi_form(literal: &str) -> String {
        literal
            .split('/')
            .map(|part| match part.strip_prefix(':') {
                Some(name) => format!("{{{name}}}"),
                None => part.to_string(),
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    /// Checks one method name of a path item.
    fn is_method(name: &str) -> bool {
        matches!(name, "get" | "post" | "put" | "delete" | "patch" | "head")
    }

    /// Reads one committed page.
    fn read_page(name: &str) -> String {
        let path = workspace_root()
            .join("docs")
            .join("reference")
            .join("api")
            .join(name);
        std::fs::read_to_string(&path).expect("committed page exists")
    }

    #[test]
    fn generated_api_pages_match_served_document() {
        // The broker serves this document at /api-docs/openapi.json.
        // Write what the broker serves to a file. Run the generator on
        // that file. The pages match the served product, not a copy.
        let served_text = openapi_spec_json();
        let dir = std::env::temp_dir().join(format!("doc-api-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir exists");
        let served_file = dir.join("served-openapi.json");
        std::fs::write(&served_file, served_text).expect("served doc writes");
        let output = generator_check_served("api", &served_file);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(
            output.status.success(),
            "generator check on served document failed:\n{}",
            String::from_utf8_lossy(&output.stdout)
        );
        let output = generator_check("api");
        assert!(
            output.status.success(),
            "generator check failed:\n{}",
            String::from_utf8_lossy(&output.stdout)
        );
        let spec: serde_json::Value =
            serde_json::from_str(served_text).expect("served document parses");
        // The router registers each served path. Route registration
        // is the production entry for the path.
        let router_src = std::fs::read_to_string(
            workspace_root()
                .join("crates")
                .join("broker-api")
                .join("src")
                .join("lib.rs"),
        )
        .expect("router source reads");
        let paths = spec
            .get("paths")
            .and_then(|paths| paths.as_object())
            .expect("served document has paths");
        // Route literals come from `.route("...", ...)` calls, with
        // `:name` mapped to `{name}`. A substring match can hit a
        // comment, so only exact literal matches count.
        let routes = router_routes(&router_src);
        // Each served operation names one router route and method.
        for (path, item) in paths {
            let item = item.as_object().expect("path item is an object");
            for method in item.keys().filter(|name| is_method(name)) {
                let found = routes
                    .iter()
                    .find(|(literal, _)| &openapi_form(literal) == path);
                let found = found.unwrap_or_else(|| panic!("router registers served path {path}"));
                assert!(found.1.contains(method), "router serves {method} {path}");
            }
        }
        // Each `/api/v1` router route names one served operation.
        // The document covers `/api/v1` and `/healthz`. Dashboard
        // assets, `/api/v5` and `/schemas` are separate surfaces.
        for (literal, methods) in &routes {
            if literal != "/healthz" && !literal.starts_with("/api/v1") {
                continue;
            }
            let path = openapi_form(literal);
            let item = paths
                .get(&path)
                .unwrap_or_else(|| panic!("served document lists router route {literal}"));
            let item = item.as_object().expect("path item is an object");
            for method in methods {
                assert!(
                    item.contains_key(method),
                    "served document lists {method} {literal}"
                );
            }
        }
        let dir = workspace_root().join("docs").join("reference").join("api");
        let mut pages = String::new();
        let mut names: Vec<String> = Vec::new();
        for entry in std::fs::read_dir(&dir).expect("api dir exists") {
            let entry = entry.expect("dir entry reads");
            let name = entry.file_name().to_string_lossy().to_string();
            names.push(name.clone());
            pages.push_str(&std::fs::read_to_string(entry.path()).expect("page reads"));
            pages.push('\n');
        }
        assert!(
            names.contains(&"index.md".to_string()),
            "api index page exists"
        );
        // Each served operation owns one section in the pages.
        for (path, item) in paths {
            let methods = item.as_object().expect("path item is an object");
            for method in methods.keys().filter(|name| is_method(name)) {
                let header = format!("### `{method} {path}`").to_uppercase();
                assert!(
                    pages.to_uppercase().contains(&header),
                    "pages document {method} {path}"
                );
            }
        }
        // Each documented section names a served operation.
        for line in pages.lines() {
            let line = line.trim();
            if !line.starts_with("### `") || !line.ends_with('`') {
                continue;
            }
            let inner = line.trim_start_matches("### `").trim_end_matches('`');
            let mut parts = inner.splitn(2, ' ');
            let method = parts.next().unwrap_or("").to_lowercase();
            let path = parts.next().unwrap_or("");
            let item = paths.get(path).expect("documented path is served");
            assert!(
                item.as_object()
                    .is_some_and(|item| item.contains_key(&method)),
                "documented method is served: {line}"
            );
        }
        let _ = read_page("index.md");
    }
}
