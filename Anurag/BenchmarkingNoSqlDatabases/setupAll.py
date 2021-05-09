import downloadPokecDataset
import text_to_json_conversion
import postgresBenchmarkTest ,mongodbBenchmarkTest, neo4jBenchmarkTest

if __name__ == "__main__":
    # download datasets,unzip, convert to json and remove txt files
    downloadPokecDataset.downloadDataset()
    text_to_json_conversion.text_to_json_Profiles_table()
    text_to_json_conversion.text_to_json_Relationship_table()

    # setup postgres (create db and insert data)
    postgresBenchmarkTest.create_tables()
    postgresBenchmarkTest.Insert_INTO_profiles_table()
    postgresBenchmarkTest.Insert_INTO_relations_table()

    # setup mongodb (create db and insert data)
    mongodbBenchmarkTest.createDB()
    mongodbBenchmarkTest.createProfileCollection()
    mongodbBenchmarkTest.createRelationsCollection()
    mongodbBenchmarkTest.insertIntoProfilesCollection()
    mongodbBenchmarkTest.insertIntoRelationsCollection()

    # setup neo4j (create db and insert data)
    neo4jBenchmarkTest.insertNodesIntoProfiles()
    neo4jBenchmarkTest.createRelationships()


