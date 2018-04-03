MAX_SECS_IDLE=$100
if [ -z "$MAX_SECS_IDLE" ]; then MAX_SECS_IDLE=1800; fi

MIN_SECS_TO_END_OF_HOUR=$2
if [ -z "$MIN_SECS_TO_END_OF_HOUR" ]; then MIN_SECS_TO_END_OF_HOUR=300; fi


(
while true  # the only way out is to SHUT DOWN THE MACHINE
do
    # get the uptime as an integer (expr can't handle decimals)
    UPTIME=$(cat /proc/uptime | cut -f 1 -d .)
    SECS_TO_END_OF_HOUR=$(expr 3600 - $UPTIME % 3600)

    
    if [ -z "$LAST_ACTIVE" ] || \
        ! which hadoop > /dev/null || \
        nice hadoop job -list 2> /dev/null | grep -q '^\s*job_' || \
        (which yarn > /dev/null && \
            nice yarn application -list 2> /dev/null | \
            grep -v 'Total number' | grep -q RUNNING)
    then
        LAST_ACTIVE=$UPTIME
    else
	# the cluster is idle! how long has this been going on?
        SECS_IDLE=$(expr $UPTIME - $LAST_ACTIVE)

        if expr $SECS_IDLE '>' $MAX_SECS_IDLE '&' \
            $SECS_TO_END_OF_HOUR '<' $MIN_SECS_TO_END_OF_HOUR > /dev/null
        then
            sudo shutdown -h now
            exit
        fi
    fi
done
# close file handles to daemonize the script; otherwise bootstrapping
# never finishes
) 0<&- &> /dev/null &