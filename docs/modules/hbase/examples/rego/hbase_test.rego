package hbase



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
    "namespace" : "hbase",
    "action" : "WRITE"
    }
}

test_namespace_admin if {
    allow with input as {
    "callerUgi" : {
      "realUser" : null,
      "userName" : "admin/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL",
      "shortUserName" : "admin",
      "primaryGroup" : null,
      "groups" : [ ],
      "authenticationMethod" : "KERBEROS",
      "realAuthenticationMethod" : "KERBEROS"
    },
    "table" : null,
    "namespace" : "developers",
    "action" : "ADMIN"
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
    "namespace" : "developers",
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
    "namespace" : "developers",
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
    "namespace" : "developers",
    "action" : "WRITE"
    }
}

test_permission_bob1 if {
    allow with input as {
    "callerUgi" : {
      "userName" : "bob/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL",
      "primaryGroup" : "admin",
    },
    "table" : {
      "namespaceAsString" : "public",
      "qualifierAsString" : "table3",
    },
    "namespace" : "public",
    "action" : "WRITE"
    }
}

test_permission_bob2 if {
    allow with input as {
    "callerUgi" : {
      "userName" : "bob/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL",
      "primaryGroup" : "admin",
    },
    "table" : {
      "namespaceAsString" : "developers",
      "qualifierAsString" : "table1",
    },
    "namespace" : "developers",
    "action" : "WRITE"
    }
}

test_permission_hbase if {
    allow with input as {
    "callerUgi" : {
      "realUser" : null,
      "userName" : "hbase/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL",
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
    "namespace" : "hbase",
    "action" : "WRITE"
    }
}

test_permission_testuser if {
    allow with input as {
    "callerUgi" : {
      "realUser" : null,
      "userName" : "testuser/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL",
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
    "namespace" : "default",
    "action" : "WRITE"
    }
}

test_permission_readonlyuser1 if {
    allow with input as {
    "callerUgi" : {
      "realUser" : null,
      "userName" : "readonlyuser1/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL",
      "shortUserName" : "readonlyuser",
      "primaryGroup" : null,
      "groups" : [ ],
      "authenticationMethod" : "KERBEROS",
      "realAuthenticationMethod" : "KERBEROS"
    },
    "table" : {
      "name" : "cHVibGljOnRlc3Q=",
      "nameAsString" : "public:test",
      "namespace" : "cHVibGlj",
      "namespaceAsString" : "public",
      "qualifier" : "dGVzdA==",
      "qualifierAsString" : "test",
      "nameWithNamespaceInclAsString" : "public:test"
    },
    "namespace" : "public",
    "action" : "READ"
    }
}

test_permission_readonlyuser2 if {
    allow with input as {
    "callerUgi" : {
      "realUser" : null,
      "userName" : "readonlyuser2/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL",
      "shortUserName" : "readonlyuser",
      "primaryGroup" : null,
      "groups" : [ ],
      "authenticationMethod" : "KERBEROS",
      "realAuthenticationMethod" : "KERBEROS"
    },
    "table" : {
      "name" : "cHVibGljOnRlc3Q=",
      "nameAsString" : "public:test",
      "namespace" : "cHVibGlj",
      "namespaceAsString" : "public",
      "qualifier" : "dGVzdA==",
      "qualifierAsString" : "test",
      "nameWithNamespaceInclAsString" : "public:test"
    },
    "namespace" : "public",
    "action" : "READ"
    }
}
