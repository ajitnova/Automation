import csv
from operator import itemgetter
from collections import defaultdict, OrderedDict
import sys
import os
sys.setrecursionlimit(1000)
csv_input_file = sys.argv[1]

# csv_file_read function generator
def csv_file_read(csv_file):
    with open(csv_file) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=';')
        for row in readCSV:
            if row[0].startswith('#'): #Skip comment lines and header
                continue
            Attribute1 = row[0]
            Attribute2 = row[1]
            AttributeX = row[2]
            attribute3 = row[3]
            Attribute3 = ' , '.join([x.strip() for x in sorted(attribute3.split(','))])
            Attribute4 = row[4]
            Attribute5 = row[5]
            attribute6 = row[6]
            Attribute6 = ' & '.join([x.strip() for x in sorted(attribute6.replace('|','&').split('&'))])
            Attribute7 = row[7]
            Attribute8 = row[8]
            Attribute9 = row[9]
            Attribute10 = row[10]
            Attribute11 = row[11]
            Attribute12 = row[12]

            StreamList = [Attribute1, Attribute2, AttributeX, Attribute3, Attribute4, Attribute5, Attribute6, Attribute7, Attribute8, Attribute9, Attribute10, Attribute11, Attribute12]
            resultList = [s.strip() for s in StreamList]

            yield resultList

# Decorator
def segregate_decorator(function):
    def wrapper(*args, **kwargs):
        segregate_list =  function(*args, **kwargs)

        print("Grouping initiated for ingestion jobs.")
        ingest_list = ingest_jobs(segregate_list)

        print("Grouping initiated for transform jobs.")
        transform_list = transform_jobs(segregate_list)

        print("grouping initiated for egression jobs.")
        export_list = export_jobs(segregate_list)

        if(len(ingest_list) > 0):
            count_checked_ingest = []
            ingest_separate_dependency = ingest_separate_dependency_condition(time_check(ingest_list))
            for groups in ingest_separate_dependency:
                for new_groups in count_limit(groups):
                    count_checked_ingest.append(new_groups)
        else:
            count_checked_ingest = None

        if (len(transform_list) > 0):
            count_checked_transform = []
            transform_separate_dependency = transorm_or_export_separate_dependency_condition(time_check(transform_list), ingest_separate_dependency)
            for groups in transform_separate_dependency:
                for new_groups in (count_limit(groups)):
                    count_checked_transform.append(new_groups)
        else:
            count_checked_transform = None

        if (len(transform_list) > 0):
            count_checked_export = []
            export_separate_dependency = transorm_or_export_separate_dependency_condition(time_check(export_list), transform_separate_dependency)
            for groups in export_separate_dependency:
                for new_groups in count_limit(groups):
                    count_checked_export.append(new_groups)
        else:
            count_checked_export = None
        csv_file_create(count_checked_ingest, count_checked_transform, count_checked_export)
    return wrapper

# Pass List of Lists segregate according to index without start_time and dependency.
@segregate_decorator
def segregate(type_list):
    group = defaultdict(list)
    for object in type_list:
        group[object[0], object[2], object[4], object[5], object[7], object[8], object[9], object[10], object[11], object[12]].append(object) #without dependency
    segregate_list = group.values()
    return segregate_list

# Ingest segregate condition
def ingest_separate_dependency_condition(time_checked_list):
    ingest_final_list = []
    for timed_group in time_checked_list:
        group = defaultdict(list)
        for object in timed_group:
            group[object[6]].append(object)
        ingest_final = group.values()
        for values in ingest_final:
            ingest_final_list.append(values)

    for i, group in enumerate([x for x in ingest_final_list if x != []]):
        for j, job in enumerate(group):
            for x_group in ingest_final_list[:i]+ingest_final_list[i+1:]:
                if (job[6][2:-1] in [job[1] for job in x_group]):
                    x_group.append(job)
                    group.pop(j)
                    ingest_final_list = recurse([x for x in ingest_final_list if x != []])
    return ingest_final_list

def recurse(ingest_final_list):
    for i, group in enumerate(ingest_final_list):
        for j, job in enumerate(group):
            for x_group in ingest_final_list[:i]+ingest_final_list[i+1:]:
                if (job[6][2:-1] in [job[1] for job in x_group]):
                    x_group.append(job)
                    group.pop(j)
                    recurse([x for x in ingest_final_list if x != []])
    return ingest_final_list

def transorm_or_export_separate_dependency_condition(time_checked_list, ingest_or_transfrom_list):
    ingest_or_transform_dict = {}
    new_transform_or_export_list = []
    external_dependancy = []
    for i, job_group in enumerate(ingest_or_transfrom_list):
        ingest_or_transform_dict["key"+str(i)] = [job[1] for job in job_group]

    for job_group in time_checked_list:
        for key, value in ingest_or_transform_dict.items():
            job_list = [job for job in job_group if job[6][2:-1] in value]
            if job_list:
                new_transform_or_export_list.append(job_list)
                for job in job_list:
                    job_group.remove(job)

    for job_group in time_checked_list:
        group = defaultdict(list)
        for object in job_group:
            group[object[6]].append(object)
        with_external_condition = group.values()
        for group_values in with_external_condition:
            external_dependancy.append(group_values)

    return new_transform_or_export_list + external_dependancy

# Three seprate function returns different job types to use later.
# Ingest jobs segregate
def ingest_jobs(segregate_list):
    ingest_list = []
    for list_of_lists in segregate_list:
        for lists in list_of_lists:
            if("ingestion" in str(lists[2]).lower()):
                ingest_list.append(list_of_lists)
                break # break here -> saves memory and time
    return ingest_list

# Transform jobs segregate
def transform_jobs(segregate_list):
    transform_list = []
    for list_of_lists in segregate_list:
        for lists in list_of_lists:
            if ("transformation" in str(lists[2]).lower()):
                transform_list.append(list_of_lists)
                break # break here -> saves memory and time
    return transform_list

# Export jobs segregate
def export_jobs(segregate_list):
    export_list = []
    for list_of_lists in segregate_list:
        for lists in list_of_lists:
            if ("egression" in str(lists[2]).lower()):
                export_list.append(list_of_lists)
                break # break here -> saves memory and time
    return export_list

def time_check(segregated_list):

    time_checked_list = []
    if(len(segregated_list) > 0):
        for index, group_job_list in enumerate(segregated_list):
            group_job_list_without_different_time = []
            diffrent_start_time_list = []

            for job in group_job_list:
                diffrent_start_time = job[3].split(',')
                if (len(diffrent_start_time) > 1):
                    diffrent_start_time_list.append(job)
                else:
                    group_job_list_without_different_time.append(job)

            if (len(diffrent_start_time_list) > 0):
                group = defaultdict(list)
                for object in diffrent_start_time_list:
                    group[object[3]].append(object)
                diffrent_start_time_job_list = group.values()

                for timed_list in diffrent_start_time_job_list:
                    time_checked_list.append(timed_list)
            start_time_initial_list_list = [[x.strip() for x in group_job_list_without_different_time[i][3].split(':')] for i in range(len(group_job_list_without_different_time))]  # Looking all list values for time attribute
            try:
                start_time_initial_list = next(s for s in start_time_initial_list_list if len(s) == 2)  # First value having time attribute
                start_time_minutes = int(start_time_initial_list[0]) * 60 + int(start_time_initial_list[1])

                tempList1 = []
                tempList2 = []
                non_time_list = []
                for index, lists in enumerate(group_job_list_without_different_time):
                    present_time_initail_list = [x.strip() for x in lists[3].split(':')]
                    if (len(present_time_initail_list) > 1):  # Time list value is not 'NULL'
                        present_minutes = int(present_time_initail_list[0]) * 60 + int(present_time_initail_list[1])
                        if ((present_minutes - start_time_minutes) <= 60):
                            tempList1.append(lists)
                        else:
                            start_time_minutes = present_minutes
                            tempList2.append(tempList1)
                            tempList1 = []
                            tempList1.append(lists)  # Boundry Condition
                    else:
                        non_time_list.append(lists)

                tempList2.append(tempList1)

                if (len(non_time_list) > 0):
                    time_checked_list.append((non_time_list))
                if (len(tempList2) > 0):
                    for timed_list in tempList2:
                        time_checked_list.append(timed_list)
            except:
                time_checked_list.append(group_job_list_without_different_time)
        return time_checked_list
    else:
        return None

# Max n jobs in one group
def count_limit(list, n = 100):
    l = []
    for i in range(0, len(list), n):
        l.append(list[i:i + n])
    return l

# CSV file creator
def csv_file_create(*args):

    for group_job_list in args:
        if(group_job_list != None):
            if(len(group_job_list)> 0):
                keys = set([jobs[0][0] for jobs in [groups for groups in group_job_list]])
                index_dict = {key: 1 for key in keys}
                for index, lists in enumerate(group_job_list):
                    for list in lists:
                        with open("GroupedJobs.csv", 'a') as myfile:
                            wr = csv.writer(myfile, delimiter=',')
                            wr.writerow([list[0] + "_" + list[2] + "_" + str(index_dict.get(list[0]))] + list)
                    with open("GroupedJobs.csv", 'a') as myfile:
                        wr = csv.writer(myfile, delimiter=',')
                        wr.writerow('#')
                    index_dict[lists[0][0]] = index_dict.get(lists[0][0]) + 1
    return 0

# Main function to interact with all different functionalities.
def main():
    try:
        os.system("rm " + os.getcwd() + "/resources/GroupedJobs.csv ")
    except:
        pass

    List = csv_file_read(csv_input_file)

    # 1. Sort jobs based on start time.
    time_sorted_list = sorted(List, key=itemgetter(3))


    # 2. Group jobs with same attributes (except start time and dependency) together.
    segregate(time_sorted_list)

    os.system("mv "+ os.getcwd()+"/GroupedJobs.csv "+ os.getcwd()+"/resources")

    print("Grouping of all jobs done successfully.")

if __name__ == '__main__':
    main()
