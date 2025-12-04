from prefect import task, Flow
import subprocess

@task
def run(cmd):
    subprocess.run(cmd, shell=True, check=True)

with Flow("daily") as flow:
    run("spark-submit --master spark://spark-master:7077 /jobs/batch_etl.py")
    run("spark-submit --master spark://spark-master:7077 /jobs/train_model.py")
    run("spark-submit --master spark://spark-master:7077 /jobs/predict.py")

if __name__ == "__main__":
    flow.run()
