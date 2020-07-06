// Michael Albrecht & Patrick Mrech

#pragma once

#include <core/compressed_column.hpp>

namespace CoGaDB{


/*!
 *  \brief     This class represents a runtime compressed column with type T, is the base class for all compressed typed column classes.
 */
    template<class T>
    class BitvectorCompressedColumn : public CompressedColumn<T>{
    public:
        /***************** constructors and destructor *****************/
        BitvectorCompressedColumn(const std::string& name, AttributeType db_type);
        virtual ~BitvectorCompressedColumn();

        virtual bool insert(const boost::any& new_Value);
        virtual bool insert(const T& new_value);
        template <typename InputIterator>
        bool insert(InputIterator first, InputIterator last);

        virtual bool update(TID tid, const boost::any& new_value);
        virtual bool update(PositionListPtr tid, const boost::any& new_value);

        virtual bool remove(TID tid);
        //assumes tid list is sorted ascending
        virtual bool remove(PositionListPtr tid);
        virtual bool clearContent();

        virtual const boost::any get(TID tid);
        //virtual const boost::any* const getRawData()=0;
        virtual void print() const throw();
        virtual size_t size() const throw();
        virtual unsigned int getSizeinBytes() const throw();

        virtual const ColumnPtr copy() const;

        virtual bool store(const std::string& path);
        virtual bool load(const std::string& path);



        virtual T& operator[](const int index);

    private:
        using BitFieldType = std::vector<uint32_t>;
        using DictType = std::pair<T, BitFieldType>;
        static constexpr uint32_t BIT_FIELD_ENTRY_SIZE = 32;
        std::vector<DictType> dictionary_;
        uint32_t entriesCount_ = 0;


        void _insert(TID tid, const T& new_value);
        const T& _get(TID) const;
        constexpr uint32_t _bitMask(const uint32_t size) const;
    };


/***************** Start of Implementation Section ******************/


    template<class T>
    BitvectorCompressedColumn<T>::BitvectorCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type), dictionary_(){

    }

    template<class T>
    BitvectorCompressedColumn<T>::~BitvectorCompressedColumn(){

    }

    template<class T>
    bool BitvectorCompressedColumn<T>::insert(const boost::any& new_value){
        if(new_value.empty()) return false;
        if(typeid(T)==new_value.type()){
            T value = boost::any_cast<T>(new_value);
            return insert(value);
        }
        return true;
    }

    template<class T>
    bool BitvectorCompressedColumn<T>::insert(const T& new_value){
        _insert(entriesCount_++, new_value);
        return true;
    }

    template<class T>
    void BitvectorCompressedColumn<T>::_insert(TID tid, const T& new_value){
        uint32_t index = 0;
        bool found = false;
        for(uint32_t i = 0;i<dictionary_.size();++i) {
            if (dictionary_[i].first == new_value) {
                found = true;
                index = i;
                break;
            }
        }

        if (!found) {
            dictionary_.emplace_back(DictType(new_value, BitFieldType()));
            index = dictionary_.size() -1;
        }

        if (tid/BIT_FIELD_ENTRY_SIZE >= dictionary_[index].second.size()) {
            for (uint32_t i = dictionary_[index].second.size();
            i<=tid/BIT_FIELD_ENTRY_SIZE;++i) {
                dictionary_[index].second.emplace_back(0);
            }
        }

        dictionary_[index].second.at(tid/BIT_FIELD_ENTRY_SIZE) |= uint32_t(1) << (tid%BIT_FIELD_ENTRY_SIZE);
        return;
    }

    template <typename T>
    template <typename InputIterator>
    bool BitvectorCompressedColumn<T>::insert(InputIterator begin, InputIterator end){
        uint32_t size = 0;
        InputIterator it = begin;

        while(it != end) {
            insert(*it);
            it++;
        }

        return true;
    }


    template<class T>
    const T& BitvectorCompressedColumn<T>::_get(TID tid) const {
        if (tid > entriesCount_) {
            throw std::runtime_error("not found");
        }
        uint32_t byteAddress = tid / BIT_FIELD_ENTRY_SIZE;
        uint32_t bitAddress = tid % BIT_FIELD_ENTRY_SIZE;

        for (auto & e : dictionary_) {
            if (e.second.size()-1 < byteAddress) {
                continue;
            }
            if ((e.second[byteAddress] & (uint32_t(1) << bitAddress)) > 0) {
                return e.first;
            }
        }
        throw std::runtime_error("not found");
    }

    template<class T>
    const boost::any BitvectorCompressedColumn<T>::get(TID tid){
        return boost::any(_get(tid));
    }

    template<class T>
    void BitvectorCompressedColumn<T>::print() const throw(){
        std::cout<<"printing col: "<<ColumnBase::getName()<<"\n";
        for(uint32_t i = 0;i<entriesCount_;++i) {
            std::cout<<"tid= "<<i<<"  data="<<(_get(i))<<"\n";
        }
        std::cout<<"-----------done-----------\n";
    }
    template<class T>
    size_t BitvectorCompressedColumn<T>::size() const throw(){
        return entriesCount_;
    }

    template<class T>
    const ColumnPtr BitvectorCompressedColumn<T>::copy() const{

        return ColumnPtr(new BitvectorCompressedColumn<T>(*this));
    }

    template<class T>
    bool BitvectorCompressedColumn<T>::update(TID tid, const boost::any& new_value){
        if (tid > entriesCount_) {
            throw std::runtime_error("not found");
        }
        uint32_t byteAddress = tid / BIT_FIELD_ENTRY_SIZE;
        uint32_t bitAddress = tid % BIT_FIELD_ENTRY_SIZE;

        for (auto & e : dictionary_) {
            BitFieldType & bf = e.second;
            if (bf.size()-1 < byteAddress) {
                continue;
            }
            if ((bf[byteAddress] & (uint32_t(1) << bitAddress)) > 0) {
                bf[byteAddress] &= ~(uint32_t(1)<<bitAddress);
                break;
            }
        }
        _insert(tid, boost::any_cast<T>(new_value));

        return false;
    }

    template<class T>
    bool BitvectorCompressedColumn<T>::update(PositionListPtr poslist, const boost::any& new_value){
        for(auto & pos : *poslist) {
            update(pos, new_value);
        }
        return true;
    }

    template<class T>
    constexpr uint32_t BitvectorCompressedColumn<T>::_bitMask(const uint32_t size) const {
        uint32_t v = 0;
        for(uint32_t i = 0;i<size;++i){
            v <<=1;
            v |= 1;
        }
        return v;
    }


    template<class T>
    bool BitvectorCompressedColumn<T>::remove(TID tid){
        uint32_t byteAddress = tid / BIT_FIELD_ENTRY_SIZE;
        uint32_t bitAddress = tid % BIT_FIELD_ENTRY_SIZE;

        // clear bit from dictionary
        for (uint32_t j = 0;j < dictionary_.size();++j) {
            BitFieldType & bf = dictionary_[j].second;
            if (bf.size()-1 < byteAddress) {
                continue;
            }
            if ((bf[byteAddress] & (uint32_t(1) << bitAddress)) > 0) {
                bf[byteAddress] &= ~(uint32_t(1)<<bitAddress);
                entriesCount_--;
                break;
            }
        }

        // shift all bits after removed position
        for (uint32_t j = 0;j < dictionary_.size();++j) {
            BitFieldType & bf = dictionary_[j].second;
            if (bf.size()-1 < byteAddress) {
                continue;
            }

            // delete bit from affected block
            uint32_t savedBits = 0;
            uint32_t bitsToSaveMask = _bitMask(bitAddress);
            savedBits = dictionary_[j].second[byteAddress] & bitsToSaveMask;
            dictionary_[j].second[byteAddress] >>= 1;
            dictionary_[j].second[byteAddress] |= savedBits;

            for (uint32_t i = byteAddress+1;i<dictionary_[j].second.size();++i) {
                // extract first bit and add it into previous bytes highest bit
                dictionary_[j].second[i-1] |= (dictionary_[j].second[i] & uint32_t(1) )<<(BIT_FIELD_ENTRY_SIZE-1);

                // shift all bits backwards
                dictionary_[j].second[i] >>=1;
            }
        }

        // check if a dictionary entry can be deleted for having not representation
        for (int j = dictionary_.size()-1;j >= 0 ;--j) {
            bool allClear = true;
            for(uint32_t i = 0;i<dictionary_[j].second.size();++i) {
                if(dictionary_[j].second[i] > 0) {
                    allClear = false;
                    break;
                }
            }
            if (allClear) {
                dictionary_.erase(dictionary_.begin() + j);
            }
        }

        return true;
    }

    template<class T>
    bool BitvectorCompressedColumn<T>::remove(PositionListPtr poslist) {
        for (auto & pos : *poslist) {
            remove(pos);
        }
        return false;
    }

    template<class T>
    bool BitvectorCompressedColumn<T>::clearContent(){
        entriesCount_ = 0;
        dictionary_.clear();
        return false;
    }

    template<class T>
    bool BitvectorCompressedColumn<T>::store(const std::string& path_){
        //string path("data/");
        std::string path(path_);
        path += "/";
        path += this->name_;
        //std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
        std::ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);

        uint32_t dictSize = dictionary_.size();
        uint8_t typeSize = sizeof(T);

        outfile.write(reinterpret_cast<char*>(&dictSize), 4);
        outfile.write(reinterpret_cast<char*>(&typeSize), 1);
        outfile.write(reinterpret_cast<char*>(&entriesCount_), 4);

        for(auto & e : dictionary_) {
            outfile.write(reinterpret_cast<char*>(&e.first), typeSize);

            uint32_t size = e.second.size();
            outfile.write(reinterpret_cast<char*>(&size), 4);

            for (auto & subEntry : e.second) {
                outfile.write(reinterpret_cast<char*>(&subEntry), BIT_FIELD_ENTRY_SIZE/8);
            }
        }

        outfile.flush();
        outfile.close();
        return true;
    }

    template<>
    bool BitvectorCompressedColumn<std::string>::store(const std::string& path_){
        //string path("data/");
        std::string path(path_);
        path += "/";
        path += this->name_;
        //std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
        std::ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);

        uint32_t dictSize = dictionary_.size();

        outfile.write(reinterpret_cast<char*>(&dictSize), 4);
        outfile.write(reinterpret_cast<char*>(&entriesCount_), 4);

        for(auto & e : dictionary_) {
            uint32_t size = e.first.size();
            outfile.write(reinterpret_cast<char*>(&size), 4);
            outfile.write(e.first.c_str(), e.first.size());

            size = e.second.size();
            outfile.write(reinterpret_cast<char*>(&size), 4);

            for (auto & subEntry : e.second) {
                outfile.write(reinterpret_cast<char*>(&subEntry), BIT_FIELD_ENTRY_SIZE/8);
            }
        }

        outfile.flush();
        outfile.close();
        return true;
    }


    template<class T>
    bool BitvectorCompressedColumn<T>::load(const std::string& path_){
        std::string path(path_);
        std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
        //string path("data/");
        path += "/";
        path += this->name_;

        //std::cout << "Opening File '" << path << "'..." << std::endl;
        std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);

        uint32_t dictionarySize = 0;
        uint8_t typeSize;

        infile.read(reinterpret_cast<char*>(&dictionarySize), 4);
        infile.read(reinterpret_cast<char*>(&typeSize), 1);
        infile.read(reinterpret_cast<char*>(&entriesCount_), 4);

        dictionary_.resize(dictionarySize);
        for(auto & e : dictionary_) {
            infile.read(reinterpret_cast<char*>(&e.first), typeSize);

            uint32_t size = 0;
            infile.read(reinterpret_cast<char*>(&size), 4);

            e.second.resize(size);
            for (auto & subEntry : e.second) {
                infile.read(reinterpret_cast<char*>(&subEntry), BIT_FIELD_ENTRY_SIZE/8);
            }
        }

        infile.close();
        return true;
    }

    template<>
    bool BitvectorCompressedColumn<std::string>::load(const std::string& path_){
        std::string path(path_);
        std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
        //string path("data/");
        path += "/";
        path += this->name_;

        //std::cout << "Opening File '" << path << "'..." << std::endl;
        std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);

        uint32_t dictionarySize = 0;

        infile.read(reinterpret_cast<char*>(&dictionarySize), 4);
        infile.read(reinterpret_cast<char*>(&entriesCount_), 4);

        dictionary_.resize(dictionarySize);
        for(auto & e : dictionary_) {
            uint32_t size = 0;
            infile.read(reinterpret_cast<char*>(&size), 4);

            char * buffer = new char[size + 1];
            buffer[size] = 0;

            infile.read(buffer, size);
            e.first = buffer;

            delete [] buffer;

            infile.read(reinterpret_cast<char*>(&size), 4);

            e.second.resize(size);
            for (auto & subEntry : e.second) {
                infile.read(reinterpret_cast<char*>(&subEntry), BIT_FIELD_ENTRY_SIZE/8);
            }
        }

        infile.close();
        return true;
    }

    template<class T>
    T& BitvectorCompressedColumn<T>::operator[](const int tid){
        const T& a = _get(tid);
        return const_cast<T&>(a);
    }

    template<class T>
    unsigned int BitvectorCompressedColumn<T>::getSizeinBytes() const throw(){
        return dictionary_.size() * sizeof(T) + ((entriesCount_/32) * 4); // imprecise but good enough
    }

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

