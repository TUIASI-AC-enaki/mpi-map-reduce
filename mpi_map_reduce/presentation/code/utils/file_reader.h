//
// Created by enaki on 05.01.2021.
//

#ifndef MPI_MAP_REDUCE_PROJECT_FILE_READER_H
#define MPI_MAP_REDUCE_PROJECT_FILE_READER_H

#include <vector>
#include <string>
#include <iostream>
#include <fstream>

namespace file_reader{
    std::vector<std::string>* get_lines(const std::string& directory, const std::string& filename){
        auto *lines = new std::vector<std::string>();
        std::string tempVariable;
        std::ifstream MyReadFile(directory + "/" + filename);

        // Use a while loop together with the getline() function to read the file line by line
        while (getline (MyReadFile, tempVariable)) {
            lines->emplace_back(tempVariable);
        }

        // Close the file
        MyReadFile.close();
        return lines;
    }
}

#endif //MPI_MAP_REDUCE_PROJECT_FILE_READER_H
