import os


class DataParser:
    def __init__(self, root_path):
        self.root_path = root_path

    def parse_users(self):
        with open(f"{self.root_path}/labeled_ids.txt") as f:
            users = f.readlines()

        users_with_data = []
        for user in users:
            user_id = user.strip()
            activities = self.parse_activities(user_id)
            users_with_data.append((user_id, activities))
        
        return users_with_data
    
    def parse_activities(self, user_id):
        user_label_path = f"{self.root_path}/Data/{user_id}/labels.txt"

        with open(user_label_path, "r") as f:
            lines = f.readlines()
        # Skip header line
        lines = lines[1:]

        labels = []
        for line in lines:
            line = line.strip().split("\t")
            start_time = line[0].replace("/", "-")
            end_time = line[1].replace("/", "-")
            activity = line[2]

            trajectory = self.parse_trajectories(user_id, start_time)
            if trajectory and self.verify_trajectory_start_end(trajectory,start_time, end_time):
                labels.append((start_time, end_time, activity, trajectory))
        return labels

    def parse_trajectories(self, user_id, datetime):
        datetime = datetime.replace("-", "").replace(":", "").replace(" ", "")
        file_path = f"{self.root_path}/Data/{user_id}/Trajectory/{datetime}.plt"

        if not os.path.exists(file_path):
            return None

        lines = []
        trajectory = []

        with open(file_path, "r") as f:
            lines = f.readlines()

        if len(lines) > 2506:
            return None

        lines = lines[6:]
        for line in lines:
            line = line.strip().split(",")

            lat = float(line[0])
            lon = float(line[1])
            alt = float(line[3])

            date_time = " ".join([line[5], line[6]])

            if alt == -777:
                alt = None

            trajectory.append((lat, lon, alt, date_time))

        return trajectory



    @staticmethod
    def verify_trajectory_start_end(trajectory, start_time, end_time):
        start_check = trajectory[0][3] == start_time
        end_check = trajectory[-1][3] == end_time
        return start_check and end_check


if __name__ == "__main__":
    parser = DataParser("dataset")
    users = parser.parse_users()
    print(users)