# earthquack_analysis

1. First step is setting up a mysql database. We can use docker and set it up quickly.
2. Once database is ready we push csv data in database table. running load_data.py will do this. [python3 load_data.py]
3. Once data is in table we can run 2nd script.[python3 py_script.py]. It will display desired output and also saves intermidiate dataframe in csv format.
