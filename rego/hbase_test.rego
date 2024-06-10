package hbase

import rego.v1

test_permission_admin if {
    allow with input as {
    "callerUgi" : {
      "userName" : "admin/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL",
      "primaryGroup" : "admin",
    },
    "table" : {
      "namespaceAsString" : "hbase",
      "qualifierAsString" : "meta",
    },
    "action" : "WRITE"
    }
}

test_permission_developers if {
    allow with input as {
    "callerUgi" : {
      "userName" : "alice/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL",
      "primaryGroup" : "admin",
    },
    "table" : {
      "namespaceAsString" : "developers",
      "qualifierAsString" : "table1",
    },
    "action" : "WRITE"
    }
}

test_permission_alice if {
    allow with input as {
    "callerUgi" : {
      "userName" : "alice/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL",
      "primaryGroup" : "admin",
    },
    "table" : {
      "namespaceAsString" : "developers",
      "qualifierAsString" : "table2",
    },
    "action" : "WRITE"
    }
}

test_no_permission_bob if {
    not allow with input as {
    "callerUgi" : {
      "userName" : "bob/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL",
      "primaryGroup" : "admin",
    },
    "table" : {
      "namespaceAsString" : "developers",
      "qualifierAsString" : "table2",
    },
    "action" : "WRITE"
    }
}

test_permission_bob if {
    allow with input as {
    "callerUgi" : {
      "userName" : "bob/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL",
      "primaryGroup" : "admin",
    },
    "table" : {
      "namespaceAsString" : "public",
      "qualifierAsString" : "table3",
    },
    "action" : "WRITE"
    }
}