# dummy_pipeline.py
import time
import logging

logging.basicConfig(
    filename=r"E:\pcr_results_pipline\dummy.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    logging.info("Dummy pipeline started.")
    for i in range(5):
        logging.info(f"Iteration {i+1}")
        time.sleep(1)
