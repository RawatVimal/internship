import downloadPokecDataset
import text_to_json_conversion
import postgresBenchmarkTest ,mongodbBenchmarkTest, neo4jBenchmarkTest

if __name__ == "__main__":
    #download datasets,unzip, convert to json and remove txt files
    downloadPokecDataset.downloadDataset()
    print("Conversion of from txt to json started..")
    text_to_json_conversion.text_to_json_Profiles_table()
    text_to_json_conversion.text_to_json_Relationship_table()
    print("Conversion of from txt to json finished..")

    # setup postgres (create db and insert data)
    print("Setting up of postgres database and insertion of data started..")
    postgresBenchmarkTest.create_database()
    postgresBenchmarkTest.create_tables()
    postgresBenchmarkTest.Insert_INTO_profiles_table()
    postgresBenchmarkTest.Insert_INTO_relations_table()
    print("Setting up of postgres database and insertion of data finished..")

    # setup mongodb (create db and insert data)
    print("Setting up of mongodb database and insertion of data started..")
    mongodbBenchmarkTest.createDB()
    mongodbBenchmarkTest.createProfileCollection()
    mongodbBenchmarkTest.createRelationsCollection()
    mongodbBenchmarkTest.insertIntoProfilesCollection()
    mongodbBenchmarkTest.insertIntoRelationsCollection()
    print("Setting up of mongodb database and insertion of data finished..")

    # setup neo4j (create db and insert data)
    print("Setting up of neo4j database and insertion of data started..")
    neo4jBenchmarkTest.insertNodesIntoProfiles()
    neo4jBenchmarkTest.createRelationships()
    print("Setting up of neo4j database and insertion of data finished..")


