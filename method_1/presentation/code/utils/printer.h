//
// Created by enaki on 08.01.2021.
//


#ifndef MPI_MAP_REDUCE_PROJECT_PRINTER_H
#define MPI_MAP_REDUCE_PROJECT_PRINTER_H

#include <utility>
#include <string>
#include <unordered_map>
#include <iostream>
#include <list>
#include <sys/stat.h>

namespace printer{
    unsigned long long get_timestamp(){
        static unsigned long long static_var = 0;
        auto now = std::chrono::system_clock::now();
        auto duration = now.time_since_epoch();
        auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration);
        return nanoseconds.count();
    }

    void create_truncated_file(std::string& filename){
        std::ofstream ofs;
        ofs.open(filename, std::ofstream::out | std::ofstream::trunc);
        ofs.close();
    }

    template <typename T>
    void cli_printer_vector(std::vector<T> vector){
        std::cout << "Size: " << vector.size() << " Vector: ";
        for (auto& key: vector){
            std::cout << key << " ";
        }
        std::cout << std::endl;
    }

    void create_word_file_direct_index(const char* directory, const char* filename, const char* word, unsigned long long count, unsigned long long timestamp, int rank){
        std::stringstream filename_builder;
        filename_builder << directory << "/" << filename << "@" << word << "@" << count << "@" << timestamp << "@" << rank;
        auto filename_string = filename_builder.str();
        create_truncated_file(filename_string);
    }

    void create_word_file_indirect_index(const char* directory, const char* filename, const char* word, unsigned long long count, unsigned long long timestamp, int rank){
        std::stringstream filename_builder;
        filename_builder << directory << "/" << word << "@" << filename << "@" << count << "@" << timestamp << "@" << rank;
        auto filename_string = filename_builder.str();
        create_truncated_file(filename_string);
    }

    void create_word_files_from_dictionary(const char* directory, std::unordered_map<std::string, unsigned long long> &word_dict, const char* filename, int rank){
        for (auto& it: word_dict) {
            create_word_file_direct_index(directory, filename, it.first.c_str(), it.second, get_timestamp(), rank);
        }
    }

    void create_word_files_from_vector(const char* directory, std::vector<std::string> word_vector, const char* filename, int rank){
        for (auto &word: word_vector){
            create_word_file_direct_index(directory, filename, word.c_str(), 1, get_timestamp(), rank);
        }
    }

    void create_word_files_from_filenames_pair(const char* directory, std::vector<std::pair<std::string, unsigned long long>>& pair_list, const char* word){
        std::stringstream filename_builder;
        filename_builder << directory << "/" << word;
        for (auto &it: pair_list){
            filename_builder << "@" << it.first << "@" << it.second;
        }
        auto filename_string = filename_builder.str();
        create_truncated_file(filename_string);
    }
}

#endif //MPI_MAP_REDUCE_PROJECT_PRINTER_H
