#include "Join.hpp"
#include <functional>

/*
 * TODO: Student implementation
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(
        Disk* disk,
        Mem* mem,
        pair<unsigned int, unsigned int> left_rel,
        pair<unsigned int, unsigned int> right_rel) {

    vector<Bucket> Buckets;
    Page *inputPage;
    Page *outMemPtr;
    for (unsigned i=0; i<MEM_SIZE_IN_PAGE-1; ++i){
        Bucket bi(disk);
        Buckets.push_back(bi);
    }
    // left relation first
    for (unsigned i=left_rel.first; i<left_rel.second; ++i){
        mem->loadFromDisk(disk, i, 0);
        inputPage = mem->mem_page(0);
        unsigned numRecords = inputPage->size();
        for (unsigned j=0; j<numRecords; ++j){
            Record curRecord = inputPage->get_record(j);
            unsigned memPlace = (curRecord.partition_hash() % (MEM_SIZE_IN_PAGE-1)) + 1;
//            cout<<"+++++++++++++"<<memPlace<<endl;
            outMemPtr = mem->mem_page(memPlace);
            if (outMemPtr->full()){
                unsigned outDiskPage = mem->flushToDisk(disk, memPlace);
                //cout << "flush in partition" << endl;
                Buckets[memPlace-1].add_left_rel_page(outDiskPage);
            }
            outMemPtr->loadRecord(curRecord);
        }
    }
    for (unsigned i=1; i<MEM_SIZE_IN_PAGE; ++i){
        outMemPtr = mem->mem_page(i);
        if (outMemPtr->size()!=0){
//            outMemPtr->print();
            unsigned outDiskPage = mem->flushToDisk(disk, i);
            //cout << "flush in partition" << endl;
            Buckets[i-1].add_left_rel_page(outDiskPage);
        }
    }

    // then right relation
    for (unsigned i=right_rel.first; i<right_rel.second; ++i){
        mem->loadFromDisk(disk, i, 0);
        inputPage = mem->mem_page(0);
        unsigned numRecords = inputPage->size();
        for (unsigned j=0; j<numRecords; ++j){
            Record curRecord = inputPage->get_record(j);
            unsigned memPlace = (curRecord.partition_hash() % (MEM_SIZE_IN_PAGE-1)) + 1;
            outMemPtr = mem->mem_page(memPlace);
            if (outMemPtr->full()){
                unsigned outDiskPage = mem->flushToDisk(disk, memPlace);
//                cout << "flush in partition" << endl;
                Buckets[memPlace-1].add_right_rel_page(outDiskPage);
            }
            outMemPtr->loadRecord(curRecord);
        }
    }
    for (unsigned i=1; i<MEM_SIZE_IN_PAGE; ++i){
        outMemPtr = mem->mem_page(i);
        if (outMemPtr->size()!=0){
//            outMemPtr->print();
            unsigned outDiskPage = mem->flushToDisk(disk, i);
            //cout << "flush in partition" << endl;
            Buckets[i-1].add_right_rel_page(outDiskPage);
        }
    }
    inputPage->reset();

    return Buckets;
}

/*
 * TODO: Student implementation
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<unsigned int> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
    vector<unsigned int> output;
    for(unsigned int i=0; i<partitions.size(); i++){
        for(unsigned int p=2; p<MEM_SIZE_IN_PAGE; p++){
            Page *ptr = mem->mem_page(p);
            ptr->reset();
        }
        if(partitions[i].num_left_rel_record<partitions[i].num_right_rel_record){
            // chose left
            for(unsigned int j=0; j<partitions[i].get_left_rel().size(); j++){
                mem->loadFromDisk(disk, partitions[i].get_left_rel()[j], 0);
                Page *inputPage = mem->mem_page(0);
                unsigned numRecords = inputPage->size();
                for(unsigned int k=0; k<numRecords; k++){
                    Record curRecord = inputPage->get_record(k);
                    unsigned hash_result = curRecord.probe_hash() % (MEM_SIZE_IN_PAGE-2);
                    Page* prt = mem->mem_page(hash_result+2);
                    prt->loadRecord(curRecord);
                }
            }
            // input page 0
            // output page 1
            for(unsigned int j=0; j<partitions[i].get_right_rel().size(); j++){
                mem->loadFromDisk(disk, partitions[i].get_right_rel()[j], 0);
                Page *inputPage = mem->mem_page(0);
                Page *outputPage = mem->mem_page(1);
                unsigned numRecords = inputPage->size();
                for(unsigned int k=0; k<numRecords; k++){
                    Record curRecord = inputPage->get_record(k);
                    unsigned hash_result = curRecord.probe_hash() % (MEM_SIZE_IN_PAGE-2);
                    Page* prt = mem->mem_page(hash_result+2);
                    for(unsigned int ii=0; ii<prt->size(); ii++){
                        if(curRecord==prt->get_record(ii)){
                            if(mem->mem_page(1)->full()){
                                unsigned outDiskPage = mem->flushToDisk(disk, 1);
                                //cout << "flush in probe" << endl;
                                output.push_back(outDiskPage);
                                outputPage->reset();
                            }
                            mem->mem_page(1)->loadPair(prt->get_record(ii), curRecord);
                        }
                    }
                }
            }
        }
        else{
            //chose right
            for(unsigned int j=0; j<partitions[i].get_right_rel().size(); j++){
                mem->loadFromDisk(disk, partitions[i].get_right_rel()[j], 0);
                Page *inputPage = mem->mem_page(0);
                unsigned numRecords = inputPage->size();
                for(unsigned int k=0; k<numRecords; k++){
                    Record curRecord = inputPage->get_record(k);
                    unsigned hash_result = curRecord.probe_hash() % (MEM_SIZE_IN_PAGE-2);
                    Page* prt = mem->mem_page(hash_result+2);
                    prt->loadRecord(curRecord);
                }
            }
            // input page 0
            // output page 1
            for(unsigned int j=0; j<partitions[i].get_left_rel().size(); j++){
                mem->loadFromDisk(disk, partitions[i].get_left_rel()[j], 0);
                Page *inputPage = mem->mem_page(0);
                Page *outputPage = mem->mem_page(1);
                unsigned numRecords = inputPage->size();
                for(unsigned int k=0; k<numRecords; k++){
                    Record curRecord = inputPage->get_record(k);
                    unsigned hash_result = curRecord.probe_hash() % (MEM_SIZE_IN_PAGE-2);
                    Page* prt = mem->mem_page(hash_result+2);
                    for(unsigned int ii=0; ii<prt->size(); ii++){
                        if(curRecord==prt->get_record(ii)){
                            if(mem->mem_page(1)->full()){
                                unsigned outDiskPage = mem->flushToDisk(disk, 1);
                                //cout << "flush in probe" << endl;
                                output.push_back(outDiskPage);
                                outputPage->reset();
                            }
                            mem->mem_page(1)->loadPair(curRecord, prt->get_record(ii));
                        }
                    }
                }
            }
        }
    }
    Page *outputPage = mem->mem_page(1);
    if(outputPage->size()!=0){
        unsigned outDiskPage = mem->flushToDisk(disk, 1);
        //cout << "flush in probe" << endl;
        output.push_back(outDiskPage);
        outputPage->reset();
    }
    return output;
}
