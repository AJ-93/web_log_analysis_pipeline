spark-submit --master k8s://kubernetes.default.svc --deploy-mode cluster \
--files spark.conf,log4j.properties \
--driver-cores 2 \
--driver-memory 3G \
--conf spark.driver.memoryOverhead=1G
main_etl.py