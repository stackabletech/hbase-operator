#!/usr/bin/env bash
#
# Run the integration test suite for this operator.
#
# If a "tests/release.yaml" file is present, it will install the operators listed
# in the release file first. The name of the test suite in that file must be "tests".
# Since each operator test suite has different dependencies, the "tests/release.yaml"
# file is not included in this repository.
#
# Optionally you can provide a specific test suite to run and even a specific
# test name.
#
# Example 1 - run all tests of the openshift suite.
#
# ./scripts/run_tests.sh --test-suite openshift --parallel 2
#
# Example 2 - run all tests that contain the word "smoke" in the openshift suite and skip resource deletion.
#
# ./scripts/run_tests.sh \
#   --test-suite openshift \
#   --test smoke \
#   --skip-delete
#

set +e

DIR_NAME=$(dirname "$0")
REPO_ROOT=$(dirname "$DIR_NAME")
TEST_ROOT="$REPO_ROOT/tests/_work"
RELEASE_FILE="$REPO_ROOT/tests/release.yaml"
STACKABLECTL_SKIP_RELEASE=""
BEKU_TEST_SUITE=""
KUTTL_TEST=""
KUTTL_SKIP_DELETE=""
KUTTL_PARALLEL=""
KUTTL_NAMESPACE=""

is_installed() {
	local command="$1"
	local install_url="$2"

	if ! which "$command" >/dev/null 2>&1; then
		echo "Command [$command] not found. To install it, please see $install_url"
		exit 1
	fi
}

install_operators() {
	if [ -n "$STACKABLECTL_SKIP_RELEASE" ]; then
		echo "Skipping operator installation"
		return
	fi

	if [ -f "$RELEASE_FILE" ]; then
		echo "Installing operators with stackablectl version: $(stackablectl --version)"
		stackablectl release install --release-file "$RELEASE_FILE" tests
	else
		echo "No tests/release.yaml found, skipping operator installation"
	fi
}

expand_test_suite() {
	# Expand the tests
	echo "Running beku version: $(beku --version)"
	if [ -z "$BEKU_TEST_SUITE" ]; then
		echo "No test suite specified, expanding all tests"
		beku
	else
		echo "Expanding test suite: $BEKU_TEST_SUITE"
		beku --suite "$BEKU_TEST_SUITE"
	fi
}

run_tests() {
	echo "Running kuttl version: $(kubectl-kuttl --version)"

	local OPTS=("test")

	if [ -n "$KUTTL_SKIP_DELETE" ]; then
		OPTS+=("--skip-delete")
	fi

	if [ -n "$KUTTL_NAMESPACE" ]; then
		OPTS+=("--namespace $KUTTL_NAMESPACE")

		# Create the namespace if it does not exist.
		# To avoid an error when the namespace already exists, we use "kubectl describe"
		# and if that fails we create the namespace.
		kubectl describe namespace "$KUTTL_NAMESPACE" || kubectl create namespace "$KUTTL_NAMESPACE"
	fi

	if [ -n "$KUTTL_PARALLEL" ]; then
		OPTS+=("--parallel $KUTTL_PARALLEL")
	fi

	if [ -n "$KUTTL_TEST" ]; then
		OPTS+=("--test=$KUTTL_TEST")
	fi

	pushd "$TEST_ROOT" || exit 1
	# Disable SC2068 because we want to pass the array as individual arguments
	# and it would break for the "--parallel n" option.
	# shellcheck disable=SC2068
	kubectl-kuttl ${OPTS[@]}
	local KUTTL_EXIT_CODE=$?
	popd || exit 1
	exit $KUTTL_EXIT_CODE
}

usage() {
	cat <<USAGE
  Usage:
     run_tests.sh [options]
  Options:
    --test-suite <test-suite>  Run a test suite from the test_definition.yaml file. Default is all tests.
    --test <test-name>         Run a specific test or a set of tests.
    --skip-delete              Skip resource deletion after the test run.
    --parallel <number>        Run tests in parallel. Default is to run all tests in parallel.
    --skip-release             Skip the operator installation.
    --namespace <namespace>    Run the tests in the specified namespace.
USAGE
}

parse_args() {
	while [[ "$#" -gt 0 ]]; do
		case $1 in
		--skip-release)
			STACKABLECTL_SKIP_RELEASE="true"
			;;
		--skip-delete)
			KUTTL_SKIP_DELETE="true"
			;;
		--parallel)
			KUTTL_PARALLEL="$2"
			shift
			;;
		--test-suite)
			BEKU_TEST_SUITE="$2"
			shift
			;;
		--test)
			KUTTL_TEST="$2"
			shift
			;;
		--namespace)
			KUTTL_NAMESPACE="$2"
			shift
			;;
		*)
			echo "Unknown parameter : $1"
			usage
			exit 1
			;;
		esac
		shift
	done
}

main() {
	parse_args "$@"

	is_installed beku "https://github.com/stackabletech/beku.py"
	is_installed stackablectl "https://github.com/stackabletech/stackable-cockpit/blob/main/rust/stackablectl/README.md"
	is_installed kubectl "https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/"
	is_installed kubectl-kuttl "https://kuttl.dev/"

	expand_test_suite
	install_operators
	run_tests
}

main "$@"
