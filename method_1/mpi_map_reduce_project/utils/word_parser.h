//
// Created by enaki on 05.01.2021.
//

#ifndef MPI_MAP_REDUCE_PROJECT_WORD_PARSER_H
#define MPI_MAP_REDUCE_PROJECT_WORD_PARSER_H

#include <string>
#include <vector>
#include <sstream>
#include <cctype>
#include <unordered_map>
#include <algorithm>

namespace word_parser{
    void to_lower_case(std::string &data){
        std::transform(data.begin(), data.end(), data.begin(),
                       [](unsigned char c){ return std::tolower(c); });
    }

    void split_string_by_separator(std::vector<std::string> &words_vector, const std::string& line, char separator=' '){
        std::stringstream string_builder;
        for (auto &character: line){
            if (character != separator){
                string_builder << character;
            } else {
                if (!string_builder.str().empty()){
                    auto word = string_builder.str();
                    to_lower_case(word);
                    words_vector.emplace_back(word);
                    string_builder.str("");
                }
            }
        }
        if (!string_builder.str().empty()){
            words_vector.emplace_back(string_builder.str());
            string_builder.str("");
        }
    }

    void split_words(std::vector<std::string> &words_vector, const std::string& line){
        std::stringstream string_builder;
        for (auto &character: line){
            if (isalpha(character)){
                string_builder << character;
            } else {
                if (!string_builder.str().empty()){
                    auto word = string_builder.str();
                    to_lower_case(word);
                    words_vector.emplace_back(word);
                    string_builder.str("");
                }
            }
        }
        if (!string_builder.str().empty()){
            words_vector.emplace_back(string_builder.str());
            string_builder.str("");
        }
    }

    void update_word_dict(std::unordered_map<std::string, unsigned long long> &word_map, std::string word, unsigned long long count){
        if (word_map.find(word) == word_map.end()){
            //std::cout << it << " not found\n\n";
            word_map[word] = count;
        } else {
            word_map[word] += count;
        }
    }
}

#endif //MPI_MAP_REDUCE_PROJECT_WORD_PARSER_H
