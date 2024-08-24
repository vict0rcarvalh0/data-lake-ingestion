from flask import Flask
from controllers.pokemon_controller import pokemon_blueprint
from data_pipeline.clickhouse_client import execute_sql_script
from middleware.logging_middleware import setup_logging

app = Flask(__name__)

setup_logging(app)

app.register_blueprint(pokemon_blueprint, url_prefix='/pokemon')

if __name__ == '__main__':
    execute_sql_script('sql/init_db.sql')
    execute_sql_script('sql/create_view.sql')
    app.run(host='0.0.0.0', port=5000)