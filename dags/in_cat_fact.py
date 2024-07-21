from airflow.decorators import dag, task 
from airflow.utils.edgemodifier import Label
from datetime import datetime
import requests


@dag(
    start_date=datetime(2024, 7, 15),
    schedule='@daily',
    catchup=False,
    description='It loads cat facts from API',
    tags=['example'],
)
def in_fact_cat():

    @task(task_id="extract_info")
    def get_cat_fact():
        r = requests.get("https://catfact.ninja/fact")
        return r.json()['fact']
    
    @task
    def cut_each_word(cat_fact):
        return cat_fact.split(" ")
    
    @task(map_index_template="{{ cat_fact_word }}")
    def process_word(constant: str, word: str):
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["cat_fact_word"] = f"Processed [{word}] with constant: {constant}"
        return word + constant
    
    @task.bash()
    def print_processed_cat_fact(processed_words):
        print(processed_words)
        return f"echo '{processed_words}'"
    
    cat_fact = get_cat_fact()
    Label("Loading data")
    words = cut_each_word(cat_fact)
    Label("Processing data")
    processed_words = process_word.partial(constant="!").expand(word=words)
    Label("Printing data")
    print_processed_cat_fact(processed_words)

in_fact_cat()
