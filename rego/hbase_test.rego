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

test_permission_hbase if {
    allow with input as {
    "callerUgi" : {
      "realUser" : null,
      "userName" : "hbase/hbase.kuttl-test-eager-javelin.svc.cluster.local@CLUSTER.LOCAL",
      "shortUserName" : "hbase",
      "primaryGroup" : null,
      "groups" : [ ],
      "authenticationMethod" : "KERBEROS",
      "realAuthenticationMethod" : "KERBEROS"
    },
    "table" : {
      "name" : "aGJhc2U6bWV0YQ==",
      "nameAsString" : "hbase:meta",
      "namespace" : "aGJhc2U=",
      "namespaceAsString" : "hbase",
      "qualifier" : "bWV0YQ==",
      "qualifierAsString" : "meta",
      "nameWithNamespaceInclAsString" : "hbase:meta"
    },
    "action" : "WRITE"
    }
}

test_permission_testuser if {
    allow with input as {
    "callerUgi" : {
      "realUser" : null,
      "userName" : "testuser/access-hbase.kuttl-test-rapid-gannet.svc.cluster.local@CLUSTER.LOCAL",
      "shortUserName" : "testuser",
      "primaryGroup" : null,
      "groups" : [ ],
      "authenticationMethod" : "KERBEROS",
      "realAuthenticationMethod" : "KERBEROS"
    },
    "table" : {
      "name" : "dGVzdA==",
      "nameAsString" : "test",
      "namespace" : "ZGVmYXVsdA==",
      "namespaceAsString" : "default",
      "qualifier" : "dGVzdA==",
      "qualifierAsString" : "test",
      "nameWithNamespaceInclAsString" : "default:test"
    },
    "action" : "WRITE"
    }
}