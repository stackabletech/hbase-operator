package hbase

import rego.v1

default allow := false
default matches_identity(identity) := false

allow if {
    some acl in acls
    matches_identity(acl.identity)
    matches_resource(input.table.namespaceAsString, input.table.qualifierAsString, acl.resource)
    action_sufficient_for_operation(acl.action, input.action)
}

# Identity mentions the (long) userName explicitly
matches_identity(identity) if {
    identity in {
        concat("", ["user:", input.callerUgi.userName])
    }
}

# Identity regex matches the (long) userName
matches_identity(identity) if {
    match_entire(identity, concat("", ["userRegex:", input.callerUgi.userName]))
}

# Identity mentions group the user is part of (by looking up using the (long) userName)
matches_identity(identity) if {
    some group in groups_for_user[input.callerUgi.userName]
    identity == concat("", ["group:", group])
}

# Allow all resources
matches_resource(namespace, table, resource) if {
    resource == ":"
}

# Resource mentions the namespace explicitly
matches_resource(namespace, table, resource) if {
    resource == concat("", [namespace, ":"])
}

# Resource mentions the namespaced table explicitly
matches_resource(namespace, table, resource) if {
    resource == concat("", [namespace, ":", table])
}

match_entire(pattern, value) if {
	# Add the anchors ^ and $
	pattern_with_anchors := concat("", ["^", pattern, "$"])

	regex.match(pattern_with_anchors, value)
}

action_sufficient_for_operation(action, operation) if {
    action_hierarchy[action][_] == action_for_operation[operation]
}

action_hierarchy := {
    "full": ["full", "rw", "ro"],
    "rw": ["rw", "ro"],
    "ro": ["ro"],
}

action_for_operation := {
    "ADMIN": "full",
    "CREATE": "full",
    "WRITE": "rw",
    "READ": "ro",
}

groups_for_user := {
    "hbase/hbase.kuttl-test-eager-javelin.svc.cluster.local@CLUSTER.LOCAL": ["admins"],
    "testuser/access-hbase.kuttl-test-rapid-gannet.svc.cluster.local@CLUSTER.LOCAL": ["admins"],
    "admin/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL": ["admins"],
    "alice/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL": ["developers"],
    "bob/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL": []
}

acls := [
    {
        "identity": "group:admins",
        "action": "full",
        "resource": ":",
    },
    {
        "identity": "group:developers",
        "action": "full",
        "resource": "developers:",
    },
    {
        "identity": "user:alice/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL",
        "action": "rw",
        "resource": "developers:table2",
    },
    {
        "identity": "user:bob/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL",
        "action": "rw",
        "resource": "developers:table1",
    },
     {
        "identity": "user:bob/test-hbase-permissions.default.svc.cluster.local@CLUSTER.LOCAL",
        "action": "rw",
        "resource": "public:table3",
    },
]