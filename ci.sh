#!/usr/bin/env bash

### shfmt -w -s -ci -sr -kp -fn ci.sh

#------------------------------------------------------------------------------
# CI Script
# Helper script for running CI jobs
#------------------------------------------------------------------------------

# Function to display usage information
display_usage()
{
	echo "Usage: $0 -t <type> -a <action> -c <code> -p <path>"
	echo "Options:"
	echo "  -t  Type (unit-test, scan-build)"
	echo "  -a  Action (configure, build, install, validate)"
	echo "  -c  Code (sofia-sip, freeswitch)"
	echo "  -p  Path to code"
	exit 1
}

# Parse command line arguments
while getopts "t:p:a:c:h" opt; do
	case $opt in
		t) TYPE="$OPTARG" ;;
		a) ACTION="$OPTARG" ;;
		c) CODE="$OPTARG" ;;
		p) PATH_TO_CODE="$OPTARG" ;;
		h) display_usage ;;
		?) display_usage ;;
	esac
done

# Function to handle sofia-sip configuration
configure_sofia_sip()
{
	./autogen.sh && ./configure.gnu || exit 1
}

# Function to handle sofia-sip build
build_sofia_sip()
{
	make -j$(nproc) || exit 1
}

# Function to handle sofia-sip installation
install_sofia_sip()
{
	make install || exit 1
}

# Function to handle sofia-sip validation
validate_sofia_sip()
{
	exit 0
}

run_as_root()
{
	if [[ $(id -u) -eq 0 ]]; then
		"$@"
	elif command -v sudo > /dev/null 2>&1; then
		sudo "$@"
	else
		"$@"
	fi
}

has_rocketmq_dependency()
{
	local has_header=1
	local has_lib=1

	if [[ -f /usr/local/include/rocketmq/DefaultMQProducer.h || -f /usr/include/rocketmq/DefaultMQProducer.h ]]; then
		has_header=0
	fi

	if command -v ldconfig > /dev/null 2>&1 && ldconfig -p 2> /dev/null | grep -q "librocketmq\\.so"; then
		has_lib=0
	elif compgen -G "/usr/local/lib/librocketmq.so*" > /dev/null || compgen -G "/usr/lib*/librocketmq.so*" > /dev/null; then
		has_lib=0
	fi

	[[ $has_header -eq 0 && $has_lib -eq 0 ]]
}

install_rocketmq_dependency()
{
	local deb_pkg="src/mod/event_handlers/mod_rocketmq/rocketmq-client-cpp-2.2.0.amd64.deb"
	local rpm_pkg="src/mod/event_handlers/mod_rocketmq/rocketmq-client-cpp-2.2.0-centos7.x86_64.rpm"
	local os_id=""
	local os_like=""

	if has_rocketmq_dependency; then
		echo "RocketMQ dependency already present, skip auto install."
		return 0
	fi

	if [[ -f /etc/os-release ]]; then
		# shellcheck disable=SC1091
		. /etc/os-release
		os_id="${ID:-}"
		os_like="${ID_LIKE:-}"
	fi

	if [[ "$os_id" =~ (debian|ubuntu) || "$os_like" =~ (debian|ubuntu) ]]; then
		if [[ ! -f "$deb_pkg" ]]; then
			echo "RocketMQ deb package not found: $deb_pkg" >&2
			return 1
		fi

		echo "Auto installing RocketMQ dependency from $deb_pkg"
		if run_as_root apt-get install -y "./$deb_pkg"; then
			return 0
		fi

		echo "apt-get local install failed, retrying with dpkg -i" >&2
		run_as_root dpkg -i "$deb_pkg"
		return $?
	fi

	if [[ "$os_id" =~ (rhel|centos|rocky|almalinux|fedora|amzn) || "$os_like" =~ (rhel|centos|fedora|suse) ]]; then
		if [[ ! -f "$rpm_pkg" ]]; then
			echo "RocketMQ rpm package not found: $rpm_pkg" >&2
			return 1
		fi

		echo "Auto installing RocketMQ dependency from $rpm_pkg"
		if command -v dnf > /dev/null 2>&1; then
			run_as_root dnf install -y "$rpm_pkg"
			return $?
		fi

		if command -v yum > /dev/null 2>&1; then
			run_as_root yum install -y "$rpm_pkg"
			return $?
		fi

		if command -v rpm > /dev/null 2>&1; then
			run_as_root rpm -Uvh --replacepkgs "$rpm_pkg"
			return $?
		fi

		echo "No supported rpm installer found on current machine." >&2
		return 1
	fi

	echo "Unsupported OS for automatic RocketMQ dependency install: ID=${os_id}, ID_LIKE=${os_like}" >&2
	return 1
}

ensure_rocketmq_dependency()
{
	echo "Checking RocketMQ build dependency..."
	install_rocketmq_dependency || {
		echo "Failed to auto install RocketMQ dependency." >&2
		return 1
	}

	if ! has_rocketmq_dependency; then
		echo "RocketMQ dependency still missing after auto install attempt." >&2
		return 1
	fi

	return 0
}

# Function to handle freeswitch configuration
configure_freeswitch()
{
	local type="$1"

	./bootstrap.sh -j || exit 1

	case "$type" in
		"unit-test")
			echo 'codecs/mod_openh264' >> modules.conf
			sed -i \
				-e '/applications\/mod_http_cache/s/^#//g' \
				-e '/formats\/mod_opusfile/s/^#//g' \
				-e '/languages\/mod_lua/s/^#//g' \
				modules.conf

			export ASAN_OPTIONS=log_path=stdout:disable_coredump=0:unmap_shadow_on_exit=1:fast_unwind_on_malloc=0

			ensure_rocketmq_dependency || exit 1

			./configure \
				--enable-address-sanitizer \
				--enable-fake-dlclose ||
				exit 1

			;;
		"scan-build")
			cp build/modules.conf.most modules.conf

			# "Enable"/"Uncomment" mods
			echo 'codecs/mod_openh264' >> modules.conf
			sed -i \
				-e '/mod_mariadb/s/^#//g' \
				-e '/mod_v8/s/^#//g' \
				modules.conf

			# "Disable"/"Comment out" mods
			sed -i \
				-e '/mod_ilbc/s/^/#/g' \
				-e '/mod_mongo/s/^/#/g' \
				-e '/mod_pocketsphinx/s/^/#/g' \
				-e '/mod_siren/s/^/#/g' \
				-e '/mod_avmd/s/^/#/g' \
				-e '/mod_basic/s/^/#/g' \
				-e '/mod_cv/s/^/#/g' \
				-e '/mod_erlang_event/s/^/#/g' \
				-e '/mod_perl/s/^/#/g' \
				-e '/mod_rtmp/s/^/#/g' \
				-e '/mod_unimrcp/s/^/#/g' \
				-e '/mod_xml_rpc/s/^/#/g' \
				modules.conf

			ensure_rocketmq_dependency || exit 1

			./configure || exit 1

			;;
		*)
			exit 1
			;;
	esac
}

# Function to handle freeswitch build
build_freeswitch()
{
	local type="$1"

	set -o pipefail

	case "$type" in
		"unit-test")
			make --no-keep-going -j$(nproc --all) |& tee ./unit-tests-build-result.txt
			build_status=${PIPESTATUS[0]}
			if [[ $build_status != "0" ]]; then
				exit $build_status
			fi

			;;
		"scan-build")
			if ! command -v scan-build-14 > /dev/null 2>&1; then
				echo "Error: scan-build-14 command not found. Please ensure clang static analyzer is installed." >&2
				exit 1
			fi

			mkdir -p scan-build

			scan-build-14 \
				--force-analyze-debug-code \
				--status-bugs \
				-o ./scan-build/ \
				make --no-keep-going -j$(nproc --all) |& tee ./scan-build-result.txt
			build_status=${PIPESTATUS[0]}

			if ! grep -siq "scan-build: No bugs found" ./scan-build-result.txt; then
				echo "scan-build: bugs found!"
				exit 1
			fi

			if [[ $build_status != "0" ]]; then
				echo "scan-build: compilation failed!"
				exit $build_status
			fi

			;;
		*)
			exit 1
			;;
	esac
}

# Function to handle freeswitch installation
install_freeswitch()
{
	make install || exit 1
}

# Function to handle freeswitch validation
validate_freeswitch()
{
	local type="$1"

	case "$type" in
		"unit-test")
			exit 0
			;;
		"scan-build")
			REPORT_PATH=$(find scan-build* -mindepth 1 -type d)
			if [ -n "$REPORT_PATH" ]; then
				echo "Found analysis report at: $REPORT_PATH"

				if command -v html2text > /dev/null 2>&1; then
					echo "Report contents:"
					html2text "$REPORT_PATH"/*.html || true
				fi

				echo "Number of issues found:"
				grep -c "<!--BUGDESC" "$REPORT_PATH"/*.html || true

				exit $([ -d "$REPORT_PATH" ])
			else
				echo "No analysis report found"
				exit 0
			fi

			;;
		*)
			exit 1
			;;
	esac
}

# Change to the code directory
if [ -n "$PATH_TO_CODE" ]; then
	cd "$PATH_TO_CODE" || exit 1
fi

# Execute appropriate flow based on code, type, and action
case "$CODE" in
	"sofia-sip")
		case "$ACTION" in
			"configure")
				configure_sofia_sip "$TYPE"
				;;
			"build")
				build_sofia_sip "$TYPE"
				;;
			"install")
				install_sofia_sip "$TYPE"
				;;
			"validate")
				validate_sofia_sip "$TYPE"
				;;
			*)
				exit 1
				;;
		esac
		;;
	"freeswitch")
		case "$ACTION" in
			"configure")
				configure_freeswitch "$TYPE"
				;;
			"build")
				build_freeswitch "$TYPE"
				;;
			"install")
				install_freeswitch "$TYPE"
				;;
			"validate")
				validate_freeswitch "$TYPE"
				;;
			*)
				exit 1
				;;
		esac
		;;
	*)
		exit 1
		;;
esac
