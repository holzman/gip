#!/bin/sh

function ask {
  echo "... to continue hit <ENTER>"
  read a
}

### for future use ###
function pick_option {
  local passed_array   # Local variable.
  passed_array=( `echo "$1"` )
  for (( i = 1 ; i < ${#passed_array[@]}+1 ; i++ ))
  do
    echo "($i) ${passed_array[$i-1]}"
  done

  while :
  do
    echo -n "Please select from the list: "
    read selection

    isInteger=0
    (test $(($selection+0)) == $selection)2>/dev/null && isInteger=1

    if [ $isInteger -eq 1 ] ; then

       if [ $selection -gt 0 ] && [ $selection -lt $((${#passed_array[@]}+1)) ] ; then
          return $(($selection-1))
          #echo "${passed_array[$selection-1]}"
          break
       else
          echo "Not in the range 1-${#passed_array[@]}"
       fi
    else
       echo "Not in the range 1-${#passed_array[@]}"
    fi
  done
}

############################################################
function entry_required {
  if [ "$1" = "UNDEFINED" ];then
    echo "... This is a required entry. Try again."
    continue
  fi
  break
}
############################################################
function validate_entry {
   entered_value=`echo $1 | tr A-Z a-z`
   valid_values="$2"
   found=0
   for valid_value in $valid_values
   do
     if [ "$valid_value" = "$entered_value" ];then
       found=1
       break
     fi
   done
   if [ $found -eq 0 ];then
     echo "...Invalid entry. Try again.
...Valid values are: $valid_values"
     continue
   fi
   break
}
############################################################
function check_if_root {
if [ `whoami` != 'root' ];then
  echo "Due to the permission of some files, you
         must be root to run this script."
  #exit 1
fi
}

############################################################
function check_vdt_location {
if [ -z "$VDT_LOCATION" ] || [ ! -e "$VDT_LOCATION" ];then
  echo "
ERROR: Invalid \$VDT_LOCATION
... please source your OSG CE \$VDT_LOCATION/setup.sh script
    or set your VDT_LOCATION variable.
"
  exit 1
fi

}




## BATCH #########################################################
#Get Batch Type
function config_batch {
    valid_batch_type="condor pbs lsf sge"
    BATCH=${BATCH:-UNDEFINED}
    while :
    do
    echo -n "Specify your BATCH system [$BATCH](condor/pbs/lsf/sge): "
    read input
    test -n "$input" && BATCH=`echo $input | tr A-Z a-z`
    validate_entry "$BATCH" "$valid_batch_type"
    done
    if [ "$BATCH" = "condor" ]; then
		if [ ! -e  "$VDT_LOCATION/vdt/etc/condor-env.sh"  ]; then
			echo
            echo "Condor doesn't seem to be installed!"
            echo "At least i could not locate $VDT_LOCATION/vdt/etc/condor-env.sh"
            echo "Please make sure condor is installed and try again!"
			echo
            echo "--- Configuration Aborted! ---"
            echo
            exit 1
        fi
    fi
    if [ "$BATCH" = "pbs" ]; then
		which qstat 2>&1> /dev/null
    	if [ $? -eq 1 ]; then
			echo
            echo "Can't find PBS-qstat location, make sure that qstat is in path."
			echo "Please make sure PBS is installed and qstat is in path, then try again!"
			echo
            echo "--- Configuration Aborted! ---"
            echo

            exit 1
		fi
    fi
    echo
}


## DISK ###########################################################
#publish gsiftp information?
function config_disk {


    valid_answers="y n"
    DISK_CHOICE="n"
    if [ "$OSG_GIP_DISK" == "1" ]; then
        DISK_CHOICE="Y"
    fi
    while :
    do
    echo -n "In addition to the SRM server, would you like to advertise a stand-alone gridftp server? (Y/n): [$DISK_CHOICE] "
    read input
    test -n "$input" && DISK_CHOICE=`echo $input | tr A-Z a-z`
    validate_entry "$DISK_CHOICE" "$valid_answers"
    done
    if [ "$DISK_CHOICE" = "n" ]; then
        DISK=0 #$DISK=1 by default
    fi
    echo
}

function check_disk {

    #check whether DISK settings are OK...
    if [ $DISK -eq 1 ]; then
       if [ -e $VDT_LOCATION/monitoring/osg-attributes.conf ]; then
          source  $VDT_LOCATION/monitoring/osg-attributes.conf

          HOSTname=$(hostname)

          SE_DISK=${OSG_GIP_SE_DISK:-UNDEFINED}
          
          if [ "$OSG_DEFAULT_SE" != "$HOSTname" ]; then

             echo "
Information about your gsiftp server
------------------------------------
gsiftp Storage Element:  A server providing an access point to data

Access Path:  The directory available on the gsiftp Storage Element

Note:  If you do not have a seperate Storage Element and your Compute Element
has a gsiftp server, enter your Compute Element hostname for gsiftp SE and the
access path on your CE.
"

             while :
	     do 
                echo -n "Please enter SE where gsiftp is running: [$SE_DISK] "
                read input
                test -n "$input" && SE_DISK="$input"
	        entry_required "$SE_DISK"
             done
    
             while :
	     do
	        DATA=${OSG_GIP_DATA:-UNDEFINED}
                echo -n "Please enter the Access Path on $SE_DISK: [$DATA] "
                read input
                test -n "$input" && DATA="$input"
	        entry_required "$DATA"
             done
          else
                  SE_DISK=$OSG_DEFAULT_SE
                  DATA=$OSG_DATA
          fi
       else
          SE_DISK="UNDEFINED"
          DATA="UNDEFINED"
       fi
    fi
    echo
}




## Enable Gums monitoring with GIP ###############################

function config_gums {

    echo "
Information status of GUMS Service
----------------------------------
Information about the status of the GUMS Server is configured to be published 
by the GIP. If you would like to turn off this option, please set the 
OSG_GIP_GUMS=\"0\" in the $VDT_LOCATION/monitoring/gip-attributes.conf.
"
    GUMS=1
    if [ "$OSG_GIP_GUMS" == "0" ]; then
        GUMS=0
    fi
}



## SRM SITE_NAME AND STORAGE_HOST ###############################
#if SRM, configure siteName, storage root, vo local dirs
#vo's are in $vdt_location/monitoring/grid3-user-vo-map.txt

function config_srm {

    echo "
Information about a possible SRM storage element
------------------------------------------------
If an SRM (Storage Resource Management) Storage Element exists that you would 
like to associate with this Compute Element, please answer 'Y'
"

    valid_answers="y n"
    SRM_CHOICE="n"
    if [ "$OSG_GIP_SRM" == "1" ]; then
        SRM_CHOICE="y"
    fi

    #SRM=${SRM:-"Y"}
    while :
    do
        echo -n "Do you want to publish your SRM information through GIP (Y/n): [$SRM_CHOICE] "
        read input
        test -n "$input" && SRM_CHOICE=`echo $input | tr A-Z a-z`
        validate_entry "$SRM_CHOICE" "$valid_answers"
    done
    if [ "$SRM_CHOICE" = "n" ]; then
        SRM=0
    else
        SRM=1
    fi
    echo
}

# SE_PATH and SE_HOST
function config_se {

    SE_NAME=${OSG_GIP_SE_NAME:-UNDEFINED}
    while :
    do
        echo -n "What is the registered OSG  sitename of your SRM Storage Element? (e.g. UIOWA-ITB): [$SE_NAME] "
        read input
        test -n "$input" && SE_NAME="$input"
        entry_required "$SE_NAME"
    done

    SE_HOST=${OSG_GIP_SE_HOST:-UNDEFINED}
    while :
    do
        echo -n "What is the hostname of your SRM Storage Element? (e.g. rsgrid3.its.uiowa.edu) : [$SE_HOST] "
        read input
        test -n "$input" && SE_HOST="$input"
        entry_required "$SE_HOST"
    done

#    SE_VERSION=${OSG_GIP_SE_VERSION:-UNDEFINED}
#    while :
#    do
#        echo -n "What is the SRM Protocol version of your SRM Storage Element? (e.g. 1, 2) : [$SE_VERSION] "
#        read input
#        test -n "$input" && SE_VERSION="$input"
#        entry_required "$SE_VERSION"
#    done


    SRM_IMPLEMENTATION_NAME=${OSG_GIP_SRM_IMPLEMENTATION_NAME:-UNDEFINED}
    while :
    do
        echo -n "What is the SRM Implementation? (e.g. dcache, bestman) : [$SRM_IMPLEMENTATION_NAME] "
        read input
        test -n "$input" && SRM_IMPLEMENTATION_NAME="$input"
        entry_required "$SRM_IMPLEMENTATION_NAME"
    done

    if [ "$SRM_IMPLEMENTATION_NAME" == "dcache" ] || [ "$SRM_IMPLEMENTATION_NAME" == "dCache" ]; then
        DYNAMIC_DCACHE=${OSG_GIP_DYNAMIC_DCACHE:-"n"}
        while :
        do
            echo -n "
GIP has the experimental ability to autodetect all dCache settings, 
but this may require additional setup outside of this script.

The separate configuration is documented at: 

https://twiki.grid.iu.edu/twiki/bin/view/InformationServices/DcacheGip

Would you like to use this alternate config instead of the standard config? 
(y/n) : [$DYNAMIC_DCACHE] "
            read input
            test -n "$input" && DYNAMIC_DCACHE="$input"
            validate_entry "$DYNAMIC_DCACHE" "y n"
        done
        if [ "$DYNAMIC_DCACHE" = "n" ]; then
            DYNAMIC_DCACHE=0
        else
            DYNAMIC_DCACHE=1
            echo
            echo "We will use the dynamic dCache providers."
            echo "Starting the configure_gip_dcache script."
            echo "---------------------------------------------------------"
            $VDT_LOCATION/gip/conf/configure_gip_dcache
            echo "---------------------------------------------------------"
            echo "Finished the configure_gip_dcache script."
            return
        fi
        echo
    fi
    export DYNAMIC_DCACHE=0
    if [ "$DYNAMIC_DCACHE" = "0" ]; then
        SRM_IMPLEMENTATION_VERSION=${OSG_GIP_SRM_IMPLEMENTATION_VERSION:-UNDEFINED}
        while :
        do
            echo -n "What is the version on your $SRM_IMPLEMENTATION_NAME Implementation? (e.g. 1.8.0-26, includes patch level) : [$SRM_IMPLEMENTATION_VERSION] "
            read input
            test -n "$input" && SRM_IMPLEMENTATION_VERSION="$input"
            entry_required "$SRM_IMPLEMENTATION_VERSION"
        done
    fi
    echo
}

function config_se_access {
    echo " Information required to Access to your SRM Server
---------------------------------------------------------"
    SE_ACCESS_NUMBER=${OSG_GIP_SE_ACCESS_NUMBER:-UNDEFINED}
    SE_ACCESS_VERSION=${OSG_GIP_SE_ACCESS_VERSION:-UNDEFINED}
    LOOP_COUNT=0

    while :
    do
        ask_question "What is the protocol version to access your SRM Storage Element? (e.g. use 1.0.0 for gsiftp, or 2.0.0 for gridftp2)" "$SE_ACCESS_VERSION"
        SE_ACCESS_VERSION=$VAL
        entry_required "$SE_ACCESS_VERSION"
    done

    while :
    do
        ask_question "How many gsiftp access points are available for this SE?" $SE_ACCESS_NUMBER
        SE_ACCESS_NUMBER=$VAL
        entry_required "$SE_ACCESS_NUMBER"
    done

    echo
    echo "Gathering Access Point Information"
    echo "---------------------------------------------------------"
    while [ $LOOP_COUNT -lt $SE_ACCESS_NUMBER ]; do
        ask_question "Enter access end point for gsiftp server? (of the form gsiftp://ftp_fqdn:ftpport)" "${OSG_GIP_SE_ACCESS_ARR[${LOOP_COUNT}]}"
        if [ "$VAL" = "UNDEFINED" ];then
            echo "... This is a required entry. Try again."
            continue
        fi
        OSG_GIP_SE_ACCESS_ARR[${LOOP_COUNT}]=$VAL
        LOOP_COUNT=$[LOOP_COUNT+1]
    done
}
function config_se_control {

    SE_CONTROL_VERSION=${OSG_GIP_SE_CONTROL_VERSION:-UNDEFINED}

    while :
    do
        ask_question "What is the SRM Protocol version of your SRM Storage Element? (e.g. 1.1.0, 2.2.0)" "$SE_CONTROL_VERSION"
        SE_CONTROL_VERSION=$VAL
        entry_required "$SE_CONTROL_VERSION"
    done
    echo
}

## SA PATH ##############################################
function config_sa {

    echo "
Information about mount points on the SRM Storage Element
---------------------------------------------------------
root directory:  The root directory is the base directory for all Virtual
Organizations (VOs).

local directory: The directory relative from the root directory.

simplified option: All Virtual Oranizations will share the same local directory.

detailed option: Seperate local directories can be entered for each 
VO.  To deny a VO access to the SRM Storage Element use the value 'UNDEFINED'.

detailed example:  A site has the following paths for VOs on an SRM.
   cms -> /pnfs/example.edu/data/cms
   cdf -> /pnfs/example.edu/data/cdf/store
   evilvo -> (no access granted)

Will translate to:
   root directory: /pnfs/example.edu/data/
   local directory (cms): cms
   local directory (cdf): cdf/store
   local directory (evilvo): UNDEFINED 
"

SA_PATH=${OSG_GIP_SA_PATH:-UNDEFINED}
    while :
    do
        #echo "(This will be your base directory, e.g if your accessible path is your cms accessible area is \"/pnfs/xxxx.edu/data1/cms\") your full access path may be \"/pnfs/xxxx.edu/\""
        echo -n "What is the Full path of the root directory for this storage area? : [$SA_PATH] "
        read input
        test -n "$input" && SA_PATH="$input"
        entry_required "$SA_PATH"
    done
    echo
}


## SA-ROOT for each VO #################################
function config_sa_roots {
    SIMPLIFIED_CHOICE=${OSG_GIP_SIMPLIFIED_SRM:-y}
    valid_answers="y n"
    processed_vos=""
    echo
    echo -n "Do you want to use a simplified version where all supported VOs will be assigned the same local directory (Y/n): [$SIMPLIFIED_CHOICE]"
    read input
    test -n "$input" && SIMPLIFIED_CHOICE=`echo $input | tr A-Z a-z`
    validate_entry "$SIMPLIFIED_CHOICE" "$valid_answers"

    FILE="$VDT_LOCATION/monitoring/osg-user-vo-map.txt"
    exec 3<&0
    exec 0<$FILE
    

    if [ "$SIMPLIFIED_CHOICE" = "n" ]; then
       echo
       #echo "Next you will set the local directories for the VO's."
       #echo "( This is the relative path, e.g if your accessible path is your cms accessible area is \"/pnfs/xxxx.edu/data1/cms\") your root access path may be \"data/cms\""
       #echo "If any VO does not have a srmcp-able directory, just hit enter)"
       #echo "If you would like to delete a previously supported VO, type UNDEFINED"
       #ask
       echo
   

     while read line
        do
        echo $line | grep \# >> /dev/null
        if [ $? -eq 1 ] && [ "$line" != "" ]; then
            vo=`echo $line | awk '{print $2}' | awk -F ^us '{print $1}'`

            if [ -z "$vo" ];then
                 vo=`echo $line | awk '{print $2}' | awk -F ^us '{print $2}'`
            fi

            processed=0
            for processed_vo in $processed_vos
            do
                if [ "$processed_vo" = "$vo" ];then
                    processed=1
                    break
                fi
            done
            if [ $processed == 1 ]; then
                continue
            fi
            processed_vos="$processed_vos $vo"
	    
	    #vo_var=$(eval echo "\$OSG_GIP_VO_${vo}_DIR")
            for (( i = 0 ; i < ${#OSG_GIP_VO_DIR[@]} ; i++ ))
            do
	        vo_name=`echo "${OSG_GIP_VO_DIR[$i]}" | awk '{split($1,a,","); print a[1]}'`
                if [ "$vo_name" = "${vo}" ]; then
                    vo_var=`echo "${OSG_GIP_VO_DIR[$i]}" | awk '{b=split($1,a,","); c=""; for (i=2; i<=b; i++) { c=c "," a[i];} print substr(c,2) }'`
		    break
		fi
            done



	    srm_dir=${vo_var}
	    srm_display=${vo_var:-UNDEFINED}
            echo -n "What is the local directory for this vo, $vo [$srm_display]? "
            read -u 3 in
	    if [ "$in" = "UNDEFINED" ]; then
                srm_dir=""
	    else 
                test -n "$in" && srm_dir="$in"
            fi
            SA_ROOTS_VO[$counter]="$vo"
            SA_ROOTS_DIR[$counter]="$srm_dir"
            ((counter++))
        fi
       done
    else

       SIMPLIFIED_CHOICE_PATH=${OSG_GIP_SIMPLIFIED_SRM_PATH:-UNDEFINED}
       while :
       do 
           echo -n "What is the local directory for all VOs [$SIMPLIFIED_CHOICE_PATH] ? "
           read -u 3 in
           test -n "$in" && SIMPLIFIED_CHOICE_PATH="$in"
	   entry_required "$SIMPLIFIED_CHOICE_PATH"
       done
       while read line
        do
        echo $line | grep \# >> /dev/null
        if [ $? -eq 1 ] && [ "$line" != "" ]; then
            vo=`echo $line | awk '{print $2}' | awk -F ^us '{print $1}'`

            if [ -z "$vo" ];then
                vo=`echo $line | awk '{print $2}' | awk -F ^us '{print $2}'`
            fi

            processed=0
            for processed_vo in $processed_vos
            do
                if [ "$processed_vo" = "$vo" ];then
                    processed=1
                    break
                fi
            done
            if [ $processed == 1 ]; then
                continue
            fi
            processed_vos="$processed_vos $vo"

            SA_ROOTS_VO[$counter]="$vo"
            SA_ROOTS_DIR[$counter]="$SIMPLIFIED_CHOICE_PATH"
            ((counter++))
        fi
       done

    fi

    exec 0<&3
    echo
}


## WRITE CONFIG ##########################################
#make backup
#mv $config_file $config_file.bak
function write_gip_config {
config_file="$VDT_LOCATION/monitoring/gip-attributes.conf"

echo "#!/bin/sh" > $config_file
echo "#This file was automatically generated by the script 'configure-osg-gip.sh'" >> $config_file
echo "#--- VARIABLES ---#" >> $config_file
echo "OSG_GIP_BATCH=\"$BATCH\"" >> $config_file
echo "OSG_GIP_SRM=$SRM" >> $config_file
echo "OSG_GIP_DISK=$DISK" >> $config_file
echo "OSG_GIP_GUMS=\"$GUMS\"" >> $config_file
echo "#---Storage Element Details---#">>$config_file
echo "OSG_GIP_SE_NAME=\"$SE_NAME\"" >> $config_file
echo "OSG_GIP_SE_HOST=\"$SE_HOST\"" >> $config_file
#echo "OSG_GIP_SE_VERSION=\"$SE_VERSION\"" >> $config_file
echo "OSG_GIP_SRM_IMPLEMENTATION_NAME=\"$SRM_IMPLEMENTATION_NAME\"" >> $config_file
echo "OSG_GIP_SRM_IMPLEMENTATION_VERSION=\"$SRM_IMPLEMENTATION_VERSION\"" >> $config_file
echo "OSG_GIP_DYNAMIC_DCACHE=\"$DYNAMIC_DCACHE\"" >> $config_file
echo "#---Storage Element Access Protocol Details---#">>$config_file
echo "OSG_GIP_SE_ACCESS_NUMBER=\"$SE_ACCESS_NUMBER\"" >> $config_file
echo "OSG_GIP_SE_ACCESS_VERSION=\"$SE_ACCESS_VERSION\"" >> $config_file
if [ ${SE_ACCESS_NUMBER} ] ; then
    for (( i = 0 ; i < $SE_ACCESS_NUMBER ; i++ ))
      do
      echo "OSG_GIP_SE_ACCESS_ARR[$i]=\"${OSG_GIP_SE_ACCESS_ARR[$i]}\"" >> $config_file
    done
fi
echo "#---Storage Element Control Protocol Details---#">>$config_file
echo "OSG_GIP_SE_CONTROL_VERSION=\"$SE_CONTROL_VERSION\"" >> $config_file
echo "#---Storage Area Details---#">>$config_file
echo "OSG_GIP_SA_PATH=\"$SA_PATH\"" >> $config_file
echo "OSG_GIP_SE_DISK=\"$SE_DISK\"" >> $config_file
echo "OSG_GIP_DATA=\"$DATA\"" >> $config_file
echo "OSG_GIP_SIMPLIFIED_SRM=\"$SIMPLIFIED_CHOICE\"" >> $config_file
echo "OSG_GIP_SIMPLIFIED_SRM_PATH=\"$SIMPLIFIED_CHOICE_PATH\"" >> $config_file
for (( i = 0 ; i < ${#SA_ROOTS_VO[@]} ; i++ ))
do
    #echo "OSG_GIP_VO_${SA_ROOTS_VO[$i]}_DIR=\"${SA_ROOTS_DIR[$i]}\"" >> $config_file
    echo "OSG_GIP_VO_DIR[$i]=\"${SA_ROOTS_VO[$i]},${SA_ROOTS_DIR[$i]}\"" >> $config_file
done

#Added for multiple SubCluster support
echo "#---Sub Cluster Variables---#">> $config_file
echo "OSG_GIP_SC_NUMBER=$OSG_GIP_SC_NUMBER" >> $config_file
for i in `seq 1 $SC_COUNT`; do
   SC_COUNT=$i
   for j in $NAME_LIST; do
      NUM_STR=`eval "echo \\$NUM_$j"`
      echo "OSG_GIP_SC_ARR[${SC_COUNT}${NUM_STR}]=\"${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_STR}]}\"" >> $config_file
   done
done
echo
echo "#--- EXPORT VARIABLES ---#" >> $config_file
echo "export OSG_GIP_BATCH" >> $config_file
echo "export OSG_GIP_DISK" >> $config_file
echo "export OSG_GIP_SE_NAME" >> $config_file
echo "export OSG_GIP_SE_HOST" >> $config_file
echo "export OSG_GIP_SA_PATH" >> $config_file
echo "export OSG_GIP_SE_DISK" >> $config_file
echo "export OSG_GIP_SE_ACCESS_NUMBER" >> $config_file
echo "export OSG_GIP_SE_ACCESS_VERSION" >> $config_file
echo "export OSG_GIP_SE_CONTROL_VERSION" >> $config_file
#echo "export OSG_GIP_SE_VERSION" >> $config_file
echo "export OSG_GIP_SRM_IMPLEMENTATION_NAME" >> $config_file
echo "export OSG_GIP_SRM_IMPLEMENTATION_VERSION" >> $config_file
echo "export OSG_GIP_DYNAMIC_DCACHE" >> $config_file
echo "export OSG_GIP_DATA" >> $config_file
echo "export OSG_GIP_GUMS" >> $config_file
echo "export OSG_GIP_SRM" >> $config_file
echo "export OSG_GIP_SIMPLIFIED_SRM" >> $config_file
echo "export OSG_GIP_SIMPLIFIED_SRM_PATH" >> $config_file
echo "export OSG_GIP_SC_NUMBER" >> $config_file

# replacement for bad chars in variable
echo "export OSG_GIP_VO_DIR" >> $config_file
#for (( i = 0 ; i < ${#SA_ROOTS_VO[@]} ; i++ ))
#do
#    echo "export OSG_GIP_VO_${SA_ROOTS_VO[$i]}_DIR" >> $config_file
#done


echo "export OSG_GIP_SC_ARR" >> $config_file
for i in `seq 1 $SC_COUNT`; do
   SC_COUNT=$i
   for j in $NAME_LIST; do
      echo "export $j" >> $config_file
   done
done
echo
echo "#--- The section below is for Internal Use Only! ---#" >> $config_file
for i in `seq 1 $SC_COUNT`; do
   SC_COUNT=$i
   for j in $NAME_LIST; do
      NUM_STR=`eval "echo \\$NUM_$j"`
      echo "$j=${NUM_STR}" >> $config_file
   done
done

echo
}

NUM_SC_NAME=01
NUM_SC_VENDOR=02
NUM_SC_MODEL=03
NUM_SC_CLOCK=04
NUM_SC_NUMPCPUS=05
NUM_SC_NUMLCPUS=06
NUM_SC_RAMSIZE=11
NUM_SC_INBOUND=21
NUM_SC_OUTBOUND=22
NUM_SC_NODES=99

#Name list contains all names from attributes above
NAME_LIST="SC_NAME SC_VENDOR SC_MODEL SC_CLOCK SC_NUMPCPUS SC_NUMLCPUS SC_RAMSIZE SC_INBOUND SC_OUTBOUND SC_NODES"

function ask_question {
	text=$1
	default=$2
	VAL=${default:-UNDEFINED}
	echo -n "$text [$VAL] "
	read in
	test -n "$in" && VAL=$in
}


function config_sc_ind {
	ask_question "What is a unique name for this Subcluster? " "${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NAME}]}"
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NAME}]=$VAL
	ask_question "What is the Vendor of the processor? (i.e. Intel, AMD) " "${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_VENDOR}]}"
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_VENDOR}]=$VAL
	ask_question "What is the Model of the processor? " "${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_MODEL}]}" 
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_MODEL}]=$VAL
	ask_question "What is the Clockspeed of the processor? "  "${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_CLOCK}]}"
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_CLOCK}]=$VAL
	ask_question "How many physical CPUs in each node? "  "${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NUMPCPUS}]}"
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NUMPCPUS}]=$VAL
	ask_question "How many logical CPUs in each node? "  "${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NUMLCPUS}]}"
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NUMLCPUS}]=$VAL
	ask_question "How much RAM is in each node (in MB)? "  "${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_RAMSIZE}]}"
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_RAMSIZE}]=$VAL
	ask_question "Is there Inbound connectivity to these nodes? (i.e. TRUE, FALSE)" "${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_INBOUND}]}"
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_INBOUND}]=$VAL
	ask_question "Is there Outbound connectivity to these nodes? (i.e. TRUE, FALSE)" "${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_OUTBOUND}]}"
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_OUTBOUND}]=$VAL
	ask_question "How many nodes in this subcluster? " "${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NODES}]}"
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NODES}]=$VAL
}

function config_sc {
	echo "
Information about your SubClusters
----------------------------------
A subcluster represents a homogeneous collection of nodes within a cluster.
A typical cluster contains only 1 subcluster (i.e. all the nodes are identical)
however some clusters contain more than 1 type of node.  These clusters
have multiple subclusters.
"

LOOP_CONT=1
OSG_GIP_SC_NUMBER=${OSG_GIP_SC_NUMBER:-1}

while [ $LOOP_CONT -eq 1 ]; do
    ask_question "How many SubClusters are available for this cluster? (i.e. 1-20)" $OSG_GIP_SC_NUMBER
    OSG_GIP_SC_NUMBER=$VAL
    if [[ $OSG_GIP_SC_NUMBER -gt 0 && $OSG_GIP_SC_NUMBER -lt 20 ]]; then
        LOOP_CONT=0
    else
        echo "Please enter the number of SubClusters (Range 1 - 20)"
    fi
done

echo "Reading information from your localhost ..."

name=$HOSTNAME
vendor=`cat /proc/cpuinfo | grep "vendor_id" | awk '{print $3}' | uniq`
model=`cat /proc/cpuinfo | grep "model name" | awk -F : '{print $2}' | uniq`
clock=`cat /proc/cpuinfo | grep "cpu MHz" | awk '{print $4}' | awk -F . '{print $1}' | uniq`
numpcpus=`cat /proc/cpuinfo | grep "processor" | wc -l`
ramsize=`cat /proc/meminfo | grep MemTotal: | awk '{print $2}'`
ramsize=$((ramsize/1000))
inbound='FALSE'
outbound='TRUE'
nodes='1'

for i in `seq 1 $OSG_GIP_SC_NUMBER`; do 
	SC_COUNT=$i
	echo
	echo "Configuring SubCluster #$SC_COUNT"
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NAME}]=${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NAME}]:-$name}
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_VENDOR}]=${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_VENDOR}]:-$vendor}
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_MODEL}]="${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_MODEL}]:-$model}"
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_CLOCK}]=${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_CLOCK}]:-$clock}
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NUMPCPUS}]=${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NUMPCPUS}]:-$numpcpus}
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NUMLCPUS}]=${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NUMLCPUS}]:-$numpcpus}
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_RAMSIZE}]=${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_RAMSIZE}]:-$ramsize}
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_INBOUND}]=${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_INBOUND}]:-$inbound}
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_OUTBOUND}]=${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_OUTBOUND}]:-$outbound}
	OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NODES}]=${OSG_GIP_SC_ARR[${SC_COUNT}${NUM_SC_NODES}]:-$nodes}

	config_sc_ind
done
}


#configure variables
function main {

    #check_permission
    check_if_root
    check_vdt_location

    if [ -e $VDT_OLD_LOCATION/monitoring/gip-attributes.conf ]; then
       source  $VDT_OLD_LOCATION/monitoring/gip-attributes.conf
    fi

    if [ -e $VDT_OLD_LOCATION/monitoring/osg-attributes.conf ]; then
       source  $VDT_OLD_LOCATION/monitoring/osg-attributes.conf
       BATCH=$OSG_JOB_MANAGER
    else
       BATCH="UNDEFINED"
    fi

    if [ -e $VDT_LOCATION/monitoring/gip-attributes.conf ]; then
       source  $VDT_LOCATION/monitoring/gip-attributes.conf
    fi

    #config_batch
    if [ -e $VDT_LOCATION/monitoring/osg-attributes.conf ]; then
       source  $VDT_LOCATION/monitoring/osg-attributes.conf
       BATCH=$OSG_JOB_MANAGER
    else
       BATCH="UNDEFINED"
    fi

    config_sc
    DISK=1 #assume publishing gsiftp info, in case publishing through srm will be disabled
    config_gums
    config_srm
    if [ "$SRM" = 1 ]; then
        config_se
        if [ "$DYNAMIC_DCACHE" = 0 ]; then
            config_se_access
            config_se_control
        else
            SE_ACCESS_NUMBER=0
        fi
        config_sa
        config_sa_roots
        config_disk
        check_disk
    else
        check_disk
    fi
    echo "writing configuration files..."
    write_gip_config
    echo

    echo "Configuring GIP..."
    $VDT_LOCATION/vdt/setup/configure_gip 
}

main 
exit
