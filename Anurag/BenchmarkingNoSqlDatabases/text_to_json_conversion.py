import json

def text_to_json_Profiles_table():

    profile_data = []

    with open('soc-pokec-profiles.txt', 'r', encoding="utf8") as data:
        for line in data:
            line = line.strip('\n')
            ldata = line.split('\t')
            if ldata[0] == "500":
                break
            temp_profile_data = {
                'user_id':ldata[0],
                'public':ldata[1],
                'completion_percentage':ldata[2],
                'gender':ldata[3],
                'region':ldata[4],
                'last_login':ldata[5],
                'registration':ldata[6],
                'AGE':ldata[7],
                'body':ldata[8],
                'I_am_working_in_field':ldata[9],
                'spoken_languages':ldata[10],
                'hobbies':ldata[11],
                'I_most_enjoy_good_food':ldata[12],
                'pets':ldata[13],
                'body_type':ldata[14],
                'my_eyesight':ldata[15],
                'eye_color':ldata[16],
                'hair_color':ldata[17],
                'hair_type':ldata[18],
                'completed_level_of_education':ldata[19],
                'favourite_color':ldata[20],
                'relation_to_smoking':ldata[21],
                'relation_to_alcohol':ldata[22],
                'sign_in_zodiac':ldata[23],
                'on_pokec_i_am_looking_for':ldata[24],
                'love_is_for_me':ldata[25],
                'relation_to_casual_sex':ldata[26],
                'my_partner_should_be':ldata[27],
                'marital_status':ldata[28],
                'children':ldata[29],
                'relation_to_children':ldata[30],
                'I_like_movies':ldata[31],
                'I_like_watching_movie':ldata[32],
                'I_like_music':ldata[33],
                'I_mostly_like_listening_to_music':ldata[34],
                'the_idea_of_good_evening':ldata[35],
                'I_like_specialties_from_kitchen':ldata[36],
                'fun':ldata[37],
                'I_am_going_to_concerts':ldata[38],
                'my_active_sports':ldata[39],
                'my_passive_sports':ldata[40],
                'profession':ldata[41],
                'I_like_books':ldata[42],
                'life_style':ldata[43],
                'music':ldata[44],
                'cars':ldata[45],
                'politics':ldata[46],
                'relationships':ldata[47],
                'art_culture':ldata[48],
                'hobbies_interests':ldata[49],
                'science_technologies':ldata[50],
                'computers_internet':ldata[51],
                'education':ldata[52],
                'sport':ldata[53],
                'movies':ldata[54],
                'travelling':ldata[55],
                'health':ldata[56],
                'companies_brands':ldata[57],
                'more':ldata[58]
            }

            profile_data.append(temp_profile_data)

    with open('soc-pokec-profiles500.json', 'w') as fp:
        json.dump(profile_data, fp, indent=4)
    #from pprint import pprint
    #pprint(profile_data)


def text_to_json_Relationship_table():

    relationship_data = []

    with open('soc-pokec-relationships.txt', 'r', encoding="utf8") as data:
        for line in data:
            line = line.strip('\n')
            ldata = line.split('\t')
            if ldata[0] == "83" and ldata[1] == "619":
                break
            temp_profile_data = {
                '_from':ldata[0],
                '_to':ldata[1]
            }
            relationship_data.append(temp_profile_data)

    with open('soc-pokec-relationship5000.json', 'w') as fp:
        json.dump(relationship_data, fp, indent=4)
    from pprint import pprint
    pprint(relationship_data)


if __name__ == "__main__":

    #text_to_json_Profiles_table()

    text_to_json_Relationship_table()