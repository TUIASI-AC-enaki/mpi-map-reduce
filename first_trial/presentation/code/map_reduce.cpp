//
// Created by enaki on 05.01.2021.
//

#include <map>
#include "utils/directory_utils.h"
#include "utils/file_reader.h"
#include "mpi.h"
#include "algorithm/shared.h"
#include "algorithm/master.h"
#include "algorithm/mapper.h"
#include "algorithm/reducer.h"


int main(int argc, char *argv[]) {
    int rank, size;
    MPI_Status status;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    // ********** INITIAL CHECK **********
    if (size != SIZE || argc != 3 || strcmp(argv[1], "-p") != 0){
        if (rank == MASTER){
            std::cout << "[ MASTER ] - Programul nu a fost rulat corespunzator." << std::endl;
            std::cout << "[ MASTER ] - Compilati cu " << SIZE << " procese." << std::endl;
            std::cout << "[ MASTER ] - Rulati executabilul astfel: ./map_reduce -p <input_path>" << std::endl;
            std::cout << "[ MASTER ] - Unde <input_path> reprezinta denumirea directorului dvs" << std::endl;
        }
        MPI_Finalize();
        return(0);
    }

    // ********** MASTER BRANCH
    if (rank == MASTER){
        auto filenames_ptr = directory_utils::get_file_names(argv[2]);
        std::cout << "\n[ MASTER ] - DIRECTORY INTRODUCED: <" << argv[2] << ">\n";
        std::cout << "\n[ MASTER ] - ***READ FILENAMES***\n";
        if (filenames_ptr->empty()){
            std::cout << "[ MASTER ] - Nu a fost gasit nici un fisier in directorul specificat\n";
        }
        create_directories();

        // TRIMITE COD DE START PENTRU WORKERI SI INCEPE TOTUL
        printf("\n[ MASTER ] - PRIMA ETAPA DE MAPARE: INDEX DIRECT\n");
        notify_wokers_of_phase(START_WORK);
        master_index_phase(filenames_ptr, MAP_DIRECT_INDEX_PHASE);

        printf("\n[ MASTER ] - PRIMA ETAPA DE REDUCERE: INDEX DIRECT\n");
        notify_wokers_of_phase(PREPARE_FOR_REDUCE_DIRECT_INDEX_PHASE);
        master_index_phase(filenames_ptr, REDUCE_DIRECT_INDEX_PHASE);

        printf("\n[ MASTER ] - A DOUA ETAPA DE MAPARE: INDEX INDIRECT\n");
        notify_wokers_of_phase(PREPARE_FOR_MAP_INDIRECT_INDEX_PHASE);
        master_index_phase(filenames_ptr, MAP_INDIRECT_INDEX_PHASE);

        printf("\n[ MASTER ] - A DOUA ETAPA DE REDUCERE: INDEX INDIRECT\n");
        notify_wokers_of_phase(PREPARE_FOR_REDUCE_INDIRECT_INDEX_PHASE);
        auto words_ptr = directory_utils::get_file_names(TEMP_FOLDER_INDIRECT_IDX_PHASE_WORDS);
        master_index_phase(words_ptr, REDUCE_INDIRECT_INDEX_PHASE);

        master_collect("output.txt", ":)");

        // TRIMITE COD DE STOP PENTRU WORKERI
        for (int worker_idx = 0; worker_idx < WORKER_SIZE; ++worker_idx){
            MPI_Send(nullptr, 0, MPI_BYTE, worker_idx, FINISHED_WORK, MPI_COMM_WORLD);
        }
        delete words_ptr;
        delete filenames_ptr;

    } else {
        // ********** WORKER BRANCH
        const int buffer_size = 256;
        char buffer[buffer_size]{0};
        std::string extract_filenames_location;
        std::vector<std::string>* words_ptr = nullptr;

        MPI_Recv(&buffer, buffer_size, MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        while (status.MPI_TAG != FINISHED_WORK) {
            log(rank, status.MPI_SOURCE, status.MPI_TAG, buffer);
            int worker_job_done_phase = FINISHED_WORK;

            switch (status.MPI_TAG) {
                case START_WORK:
                    worker_job_done_phase = FINISHED_PREPARING;
                    break;
                case MAP_DIRECT_INDEX_PHASE:
                    mapper_direct_index_phase(argv[2], buffer, rank);
                    break;
                case PREPARE_FOR_REDUCE_DIRECT_INDEX_PHASE:
                    extract_filenames_location = TEMP_FOLDER_DIRECT_IDX_PHASE_MAPPERS;
                    break;
                case REDUCE_DIRECT_INDEX_PHASE:
                    reduce_direct_index_phase(words_ptr, buffer, rank);
                    break;
                case PREPARE_FOR_MAP_INDIRECT_INDEX_PHASE:
                    extract_filenames_location = TEMP_FOLDER_DIRECT_IDX_PHASE_REDUCERS;
                    break;
                case MAP_INDIRECT_INDEX_PHASE:
                    mapper_indirect_index_phase(words_ptr, buffer, rank);
                    break;
                case PREPARE_FOR_REDUCE_INDIRECT_INDEX_PHASE:
                    extract_filenames_location = TEMP_FOLDER_INDIRECT_IDX_PHASE_MAPPERS;
                    break;
                case REDUCE_INDIRECT_INDEX_PHASE:
                    reduce_indirect_index_phase(words_ptr, buffer);
                    break;
                default:
                    break;
            }

            if (is_preparing_phase(status.MPI_TAG)){
                delete words_ptr;
                words_ptr = directory_utils::get_file_names(extract_filenames_location);
                std::sort((*words_ptr).begin(), (*words_ptr).end());
                worker_job_done_phase = FINISHED_PREPARING;
            }

            // NOTIFICA MASTER CA AM TERMINAT CE AM AVUT DE FACUT
            MPI_Send(nullptr, 0, MPI_BYTE, MASTER, worker_job_done_phase, MPI_COMM_WORLD);

            // WAITING FOR COMMANDS FROM MY MASTER <3
            MPI_Recv(&buffer, 100, MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        }
        if (status.MPI_TAG == FINISHED_WORK){
            log(rank, status.MPI_SOURCE, status.MPI_TAG);
            delete words_ptr;
        }
    }
    MPI_Finalize();
    return(0);
}
