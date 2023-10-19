import main
from main import insight_sales
from prefect import Flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule


deployment_oqc = Deployment.build_from_flow(
    flow = insight_sales ,
    name="inside_sale_ond_growth",
    schedule=(CronSchedule(cron="0 9 * * *", timezone="Asia/Kolkata")),
    version=1, 
    work_queue_name="insight_sales",
    tags = ['ond'], 
)






if __name__ == "__main__":
    deployment_oqc.apply()
