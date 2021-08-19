#!/bin/bash
set +x
dcm4chepath='/home/ubuntu/dcm4che/dcm4che-5.23.3/bin'
storage='/home/ubuntu/dcm4che/storage'
logfile='/home/ubuntu/dcm4che/server.log'
aet='aidocbox'
aec='ORTHANC'
port='4243'
target_ip='172.31.29.53'
sleep_in_seconds = 10

secs=1800    # Set interval (duration) in seconds.
endTime=$(( $(date +%s) + secs )) # Calculate end time.

while [ $(date +%s) -lt $endTime ];
do

   find $storage -type d -exec $dcm4chepath/storescu -c $aet@$target_ip:$port {} \;
   echo "sleep for " + $sleep_in_seconds
   sleep $sleep_in_seconds
done

#for i in [1..100]
#do
#sh $dcm4chepath/storescu -c ${aet}@${target_ip}:${port} ${storage} >> ${logfile}
#done

#/home/ubuntu/dcm4che/dcm4che-5.23.3/bin/storescu -c aidocbox@172.31.29.53:4242 '/home/ubuntu/dcm4che/storage/study_1.2.826.0.1.3680043.8.498.90350753967653843989447509989230912296/CT.1.2.826.0.1.3680043.9.6883.1.99995555461474442203568496879763444.dcm'