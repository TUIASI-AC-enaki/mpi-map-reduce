//
// Created by enaki on 05.01.2021.
//

#ifndef MPI_MAP_REDUCE_PROJECT_DIRECTORY_UTILS_H
#define MPI_MAP_REDUCE_PROJECT_DIRECTORY_UTILS_H

#include <vector>
#include <iostream>
#include <string>
#include <cstring>
#include <dirent.h>

namespace directory_utils{
    std::vector<std::string>* get_file_names(const std::string& directory_path){
        DIR *dr;
        struct dirent *en = nullptr;
        dr = opendir(directory_path.c_str()); //open all directory
        auto *file_names = new std::vector<std::string>();
        if (dr) {
            while ((en = readdir(dr)) != nullptr) {
                // std::cout<<"\n"<<en->d_name; //print all directory name
                if (strcmp(en->d_name, ".") != 0 && strcmp(en->d_name, "..") != 0)
                    file_names->emplace_back(en->d_name);
            }
            closedir(dr); //close all directories
        }
        delete en;
        return file_names;
    }
}


#endif //MPI_MAP_REDUCE_PROJECT_DIRECTORY_UTILS_H
