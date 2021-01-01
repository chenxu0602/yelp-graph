import json
from pathlib import Path
import pandas as pd

def process_user(
    input=Path("json/yelp_academic_dataset_user.json"),
    output_dir=Path("csv")):
    """ Process the business json file """

    user_dict = {
        "user_id": [],
        "name": [],
    }

    friend_dict = {
        "user_id": [],
        "friend_id": [],
    }

    with open(input, "r") as f:
        for line in f:
            data = json.loads(line)
            if "user_id" not in data or data["user_id"] is None:
                continue

            if "name" not in data or data["name"] is None:
                continue

            user_id = data["user_id"].strip()
            name = data["name"].strip()

            user_dict["user_id"].append(user_id)
            user_dict["name"].append(name)

            if "friends" in data and data["friends"] is not None:
                for friend in data["friends"].split(','):
                    if friend is None: continue
                    friend = friend.strip()
                    if friend in ["", "None"]: continue
                    friend_dict["user_id"].append(user_id)
                    friend_dict["friend_id"].append(friend)
            
    df_user = pd.DataFrame(user_dict)
    df_friend = pd.DataFrame(friend_dict)

    output_dir.mkdir(parents=True, exist_ok=True)
    user = output_dir / "user.csv"
    friend = output_dir / "friend.csv"

    print(f"Saving to {user} ...")
    df_user.to_csv(user, index=False)
    print(f"Saving to {friend} ...")
    df_friend.to_csv(friend, index=False)

    return True

def process_review(
    input=Path("json/yelp_academic_dataset_review.json"),
    output_dir=Path("csv")):
    """ Process the review json file """

    review_dict = {
        "review_id": [],
        # "text": [],
        "stars": [],
        "date": [],
    }

    user_review_dict = {
        "user_id": [],
        "review_id": [],
    }

    review_business_dict = {
        "review_id": [],
        "business_id": [],
    }

    with open(input, "r") as f:
        for line in f:
            data = json.loads(line)

            if "business_id" not in data or data["business_id"] is None: continue
            if "user_id" not in data or data["user_id"] is None: continue
            if "review_id" not in data or data["review_id"] is None: continue

            business_id = data["business_id"].strip()
            user_id = data["user_id"].strip()
            review_id = data["review_id"].strip()

            stars = data["stars"]
            dt = data["date"]

            if isinstance(stars, float) and isinstance(dt, str) and len(dt) == 19:
                review_dict["review_id"].append(review_id)
                review_dict["stars"].append(stars)
                review_dict["date"].append(dt)

                user_review_dict["user_id"].append(user_id)
                user_review_dict["review_id"].append(review_id)

                review_business_dict["review_id"].append(review_id)
                review_business_dict["business_id"].append(business_id)


    df_review = pd.DataFrame(review_dict)
    df_user = pd.DataFrame(user_review_dict)
    df_business = pd.DataFrame(review_business_dict)

    output_dir.mkdir(parents=True, exist_ok=True)
    review = output_dir / "review.csv"
    user = output_dir / "user_review.csv"
    business = output_dir / "review_business.csv"

    print(f"Saving to {review} ...")
    df_review.to_csv(review, index=False)
    print(f"Saving to {user} ...")
    df_user.to_csv(user, index=False)
    print(f"Saving to {business} ...")
    df_business.to_csv(business, index=False)

    return True


def process_business(
    input=Path("json/yelp_academic_dataset_business.json"),
    output_dir=Path("csv")):
    """ Process the business json file """

    business_dict = {
        "business_id": [],
        "name": [],
        "address": [],
        "city": [],
        "state": [],
        "postal_code": [],
        "latitude": [],
        "longitude": [],
        "stars": [],
    }

    business_in_city_dict = {
        "business_id": [],
        "city": [],
    }

    business_in_category_dict = {
        "business_id": [],
        "category": [],
    }

    with open(input, "r") as f:
        for line in f:
            data = json.loads(line)
            if "business_id" not in data or data["business_id"] is None:
                continue

            business_id = data["business_id"].strip()

            for key in business_dict.keys():
                if key in data:
                    val = data[key]
                    if isinstance(val, str):
                        val = val.strip()
                    business_dict[key].append(val)
                else:
                    business_dict[key].append(None)

            if "city" in data and data["city"] is not None:
                city = data["city"].replace('"', '')
                city = city.strip()
                city = city.strip(',')
                city = city.strip()
                if city == "": city = None
                business_in_city_dict["business_id"].append(business_id)
                business_in_city_dict["city"].append(city)

            if "categories" in data and data["categories"] is not None:
                for category in data["categories"].split(','):
                    category = category.replace('"', '')
                    category = category.strip()
                    category = category.strip(',&')
                    category = category.strip()
                    business_in_category_dict["business_id"].append(business_id)
                    business_in_category_dict["category"].append(category)


    df_business = pd.DataFrame(business_dict)
    df_city = pd.DataFrame(business_in_city_dict)
    df_category = pd.DataFrame(business_in_category_dict)


    output_dir.mkdir(parents=True, exist_ok=True)
    business = output_dir / "business.csv"
    in_city = output_dir / "business_in_city.csv"
    in_category = output_dir / "business_in_category.csv"

    city = output_dir / "city.csv"
    category = output_dir / "category.csv"

    print(f"Saving to {business} ...")
    df_business.to_csv(business, index=False)
    print(f"Saving to {in_city} ...")
    df_city.to_csv(in_city, index=False)
    print(f"Saving to {in_category} ...")
    df_category.to_csv(in_category, index=False)

    print(f"Saving to {city} ...")
    df_city[["city"]].drop_duplicates().sort_values(by="city").to_csv(city, index=False)
    print(f"Saving to {category} ...")
    df_category[["category"]].drop_duplicates().sort_values(by="category").to_csv(category, index=False)

    return True

# def process(filename, func):
#     print(f"Processing {filename} with {func.__name__} ...")
#     with open(filename, "r") as f:
#         for line in f:
#             data = json.loads(line)
#             print(data)
#             break