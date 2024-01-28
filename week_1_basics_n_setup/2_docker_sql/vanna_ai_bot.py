import argparse
from types import ModuleType as Module
import vanna as vn

def set_config(email: str, vanna_model_name: str) -> Module("vanna"):
    api_key = vn.get_api_key(email)

    vn.set_api_key(api_key)
    vn.set_model(vanna_model_name)
    # print(vn.get_models())

    return vn

def train(vn: Module("vanna"), hostname: str, port: str, db: str, user: str, password: str):
    vn.connect_to_postgres(host=hostname, port=port, dbname=db, user=user, password=password)

    # The information schema query may need some tweaking depending on your database. This is a good starting point.
    df_information_schema = vn.run_sql("SELECT * FROM INFORMATION_SCHEMA.COLUMNS")

    # This will break up the information schema into bite-sized chunks that can be referenced by the LLM
    plan = vn.get_training_plan_generic(df_information_schema)
    print(plan)

    # If you like the plan, then uncomment this and run it to train
    vn.train(plan=plan)

    # # At any time you can inspect what training data the package is able to reference
    # training_data = vn.get_training_data()
    # print(training_data)

def ask(vn: Module("vanna"), question: str):
    vn.ask(question=question)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ask Vanna a question to get an postgres SQL answer')
    parser.add_argument('--email', required=True, help='email for vanna.ai')
    parser.add_argument('--model', required=True, help='model name for vanna.ai')
    parser.add_argument('--is_training', required=True, help='is this a training run? if already trained, then set to False')
    parser.add_argument('--host', required=True, help='hostname for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database for postgres')
    parser.add_argument('--user', required=True, help='user for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--question', required=True, help='question to ask Vanna')

    args = parser.parse_args()

    vn = set_config(email=args.email, vanna_model_name=args.model)
    if args.is_training == "True":
        train(vn,
              hostname=args.host,
              port=args.port,
              db=args.db,
              user=args.user,
              password=args.password)  # only run this once, otherwise you update the data
    ask(vn, args.question)
