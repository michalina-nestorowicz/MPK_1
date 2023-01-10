Project consist of two components: \
*app.py* - Functions as REST API, should always run in the background \
*main_scrapper.py* - Downloads files and updates MPK database, should be run periodically for example every day at 1 AM. \
*main_scrapper.py* was created using TDD, with unit tests in *test_datascrapper.py* and *test_database_update.py* files.
