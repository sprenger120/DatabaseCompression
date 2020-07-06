
/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.
 */

#pragma once

#include <core/compressed_column.hpp>

#include <vector>

namespace CoGaDB{
	

/*!
 *  \brief     This class represents a dictionary compressed column with type T, is the base class for all compressed typed column classes.
 */	
template<class T>
class DictionaryCompressedColumn : public CompressedColumn<T>{
	public:
	/***************** constructors and destructor *****************/
	DictionaryCompressedColumn(const std::string& name, AttributeType db_type);
	virtual ~DictionaryCompressedColumn();

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
    std::vector<uint32_t> _surrogates;
    uint32_t _surrogateEntries = 0;
    uint32_t _currSurrogateBitSize = 4;
    std::vector<T> _dictionary;

    // check if surrogate bit size is still sufficient for number column entries
    void _checkAndResizeSurrogates(int numNewEntries);

    constexpr uint32_t _bitMask(uint32_t size) const;
    uint32_t _lookupInSurrogates(uint32_t tid) const;
    void __insertSurrogate(std::vector<uint32_t>& target, uint32_t value,
                           uint32_t tid, uint8_t entrysize) const;
    void _insertSurrogate(uint32_t value);
    uint32_t _intPow(uint32_t x,uint32_t p) const;
};


/***************** Start of Implementation Section ******************/

	
	template<class T>
	DictionaryCompressedColumn<T>::DictionaryCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type), _surrogates(), _dictionary(){

	}

	template<class T>
	DictionaryCompressedColumn<T>::~DictionaryCompressedColumn(){

	}

    template<class T>
	void DictionaryCompressedColumn<T>::_checkAndResizeSurrogates(int numNewEntries) {
	    if (_dictionary.size() + numNewEntries < _intPow(2, _currSurrogateBitSize)-1) {
	        return;
	    }

        uint8_t newSurrogateSize = static_cast<uint8_t>(
                ceil(log2(static_cast<double>(_dictionary.size() + numNewEntries)))) + 1;



	    std::vector<uint32_t> newSurrogates(_surrogates.size());
        for (uint32_t tid = 0;tid<_surrogateEntries;++tid) {
            __insertSurrogate(newSurrogates, _lookupInSurrogates(tid), tid, newSurrogateSize);
        }

        _surrogates = newSurrogates;
        _currSurrogateBitSize = newSurrogateSize;
	}

    template<class T>
    uint32_t DictionaryCompressedColumn<T>::_intPow(uint32_t x, const uint32_t p) const {
        if (p == 0) return 1;
        if (p == 1) return x;
        return x * _intPow(x, p-1);
    }

    template<class T>
    constexpr uint32_t DictionaryCompressedColumn<T>::_bitMask(const uint32_t size) const {
        uint32_t v = 0;
        for(uint32_t i = 0;i<size;++i){
            v <<=1;
            v |= 1;
        }
        return v;
    }

    template<class T>
    uint32_t DictionaryCompressedColumn<T>::_lookupInSurrogates(uint32_t tid) const {
        uint64_t bitAddressStart = tid * _currSurrogateBitSize;
        uint64_t bitAddressEnd = ((tid+1) * _currSurrogateBitSize)-1;
        uint32_t byteAddressStart = bitAddressStart/32;
        uint32_t byteAddressEnd = bitAddressEnd/32;

        uint8_t bitStartIndex = bitAddressStart%32;

        if (byteAddressEnd >= _surrogates.size()) {
            throw std::runtime_error("out of range");
        }

        // split between entry and previous one?
        if (byteAddressStart == byteAddressEnd) {
            return (_surrogates[byteAddressStart] & (_bitMask(_currSurrogateBitSize)<<bitStartIndex))>>bitStartIndex;
        } else {
            uint64_t temp = static_cast<uint64_t>(_surrogates[byteAddressStart]) |
                            (static_cast<uint64_t>(_surrogates[byteAddressEnd])<<32);

            return (temp & (static_cast<uint64_t>(_bitMask(_currSurrogateBitSize))<<bitStartIndex))>>bitStartIndex;
        }
    }

    template<class T>
    void DictionaryCompressedColumn<T>::__insertSurrogate(std::vector<uint32_t>& target, uint32_t value,
            uint32_t tid, uint8_t entrysize) const
    {
        uint64_t bitAddressStart = tid * entrysize;
        uint64_t bitAddressEnd = ((tid+1) * entrysize)-1;
        uint32_t byteAddressStart = bitAddressStart/32;
        uint32_t byteAddressEnd = bitAddressEnd/32;

        uint8_t bitStartIndex = bitAddressStart%32;

        if (byteAddressEnd >= target.size()) {
            target.emplace_back(0);
        }

        // split between entry and previous one?
        if (byteAddressStart == byteAddressEnd) {
            target.at(byteAddressStart) &= ~(_bitMask(entrysize)<<(bitStartIndex));
            target.at(byteAddressStart) |= (value<<bitStartIndex);
        } else {
            uint64_t temp = static_cast<uint64_t>(target.at(byteAddressStart)) |
                    (static_cast<uint64_t>(target.at(byteAddressEnd))<<32);

            temp &= ~(static_cast<uint64_t>(_bitMask(entrysize))<<bitStartIndex);
            temp |= (static_cast<uint64_t>(value)<<bitStartIndex);

            target[byteAddressStart] = static_cast<uint32_t>(temp);
            target[byteAddressEnd] = static_cast<uint32_t>(temp>>32);
        }
    }

    template<class T>
    void DictionaryCompressedColumn<T>::_insertSurrogate(uint32_t value) {
        __insertSurrogate(_surrogates, value, _surrogateEntries++, _currSurrogateBitSize);
    }


	template<class T>
	bool DictionaryCompressedColumn<T>::insert(const boost::any& new_value){
        if(new_value.empty()) return false;
        if(typeid(T)==new_value.type()){
            T value = boost::any_cast<T>(new_value);
            return insert(value);
        }
        return true;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::insert(const T& value){
	    // lookup if value is already in dictionary
	    uint32_t dictIndex = 0;
	    bool found = false;

	    for(uint32_t i = 0;i<_dictionary.size();++i) {
	        if (_dictionary[i] == value) {
	            dictIndex = i;
	            found = true;
	            break;
	        }
	    }

	    if (!found) {
	        _dictionary.push_back(value);
            dictIndex = _dictionary.size()-1;
	    }

	    // add surrogate
        _checkAndResizeSurrogates(1);
        _insertSurrogate(dictIndex);

		return true;
	}

	template <typename T> 
	template <typename InputIterator>
	bool DictionaryCompressedColumn<T>::insert(InputIterator begin, InputIterator end){
	    uint32_t size = 0;
	    InputIterator it = begin;

	    while(it != end) {
	        size++;
	        it++;
	    }

	    _checkAndResizeSurrogates(size);

        while(it != end) {
            _insertSurrogate(*it);
            it++;
        }

		return true;
	}

	template<class T>
	const boost::any DictionaryCompressedColumn<T>::get(TID tid){
	    if (tid >= _surrogateEntries) {
            throw  std::runtime_error("out of range");
	    }

	    uint32_t dictIndex = _lookupInSurrogates(tid);
		return boost::any(_dictionary[dictIndex]);
	}

	template<class T>
	void DictionaryCompressedColumn<T>::print() const throw(){
        std::cout<<"printing: "<<this->name_<<"\n";
	    for(uint32_t tid=0;tid<_surrogateEntries;++tid) {
	        std::cout<<"tid= "<<tid<<"\tvalue="<<_dictionary[_lookupInSurrogates(tid)]<<"\n";
	    }
	}

	template<class T>
	size_t DictionaryCompressedColumn<T>::size() const throw(){
		return  _surrogateEntries;
	}
	template<class T>
	const ColumnPtr DictionaryCompressedColumn<T>::copy() const{

		return ColumnPtr(new DictionaryCompressedColumn<T>(*this));
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::update(TID tid, const boost::any& newValue){
	    try {
            _dictionary[_lookupInSurrogates(tid)] = boost::any_cast<T>(newValue);
        } catch(const std::runtime_error&e){
	        return false;
	    }
		return true;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::update(PositionListPtr positions, const boost::any& newValue){
        try {
            for (const auto &p : *positions) {
                _dictionary[_lookupInSurrogates(p)] = boost::any_cast<T>(newValue);
            }
        } catch(const std::runtime_error&e){
            return false;
        }
        return true;
	}
	
	template<class T>
	bool DictionaryCompressedColumn<T>::remove(TID toBeRemoved){
	    try {
	        _lookupInSurrogates(toBeRemoved);
	    } catch(const std::runtime_error& e) {
	        return false;
	    }

        // not shrinking surrogate size to avoid thrashing when elements are added and removed constantly
        // on the threshold
        // not cleaning dictionary because that is really messy

        std::vector<uint32_t> newSurrogates;
	    uint32_t newTid = 0;
        for (uint32_t tid = 0;tid<_surrogateEntries;++tid) {
            if (tid == toBeRemoved) {
                continue;
            }
            __insertSurrogate(newSurrogates, _lookupInSurrogates(tid), newTid++, _currSurrogateBitSize);
        }

        _surrogates = newSurrogates;
        _surrogateEntries = newTid;
		return true;
	}
	
	template<class T>
	bool DictionaryCompressedColumn<T>::remove(PositionListPtr list){
        // not shrinking surrogate size to avoid thrashing when elements are added and removed constantly
        // on the threshold
        // not cleaning dictionary because that is really messy
        bool someEntriesDeleted = false;

        std::vector<uint32_t> newSurrogates;
        uint32_t newTid = 0;
        for (uint32_t tid = 0;tid<_surrogateEntries;++tid) {
            bool found = false;
            for(const auto & e : *list) {
                if (tid == e) {
                    found = true;
                    break;
                }
            }

            if (found) {
                someEntriesDeleted = true;
                continue;
            }
            __insertSurrogate(newSurrogates, _lookupInSurrogates(tid), newTid++, _currSurrogateBitSize);
        }

        _surrogates = newSurrogates;
        _surrogateEntries = newTid;
        return someEntriesDeleted;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::clearContent(){
	    _dictionary.clear();
	    _surrogates.clear();
	    _surrogateEntries = 0;
		return true;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::store(const std::string& path_){
        //string path("data/");
        std::string path(path_);
        path += "/";
        path += this->name_;
        //std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
        std::ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);

        uint32_t dictSize = _dictionary.size();
        uint32_t surrSize = _surrogates.size();
        uint8_t typeSize = sizeof(T);

        outfile.write(reinterpret_cast<char*>(&_surrogateEntries), 4);
        outfile.write(reinterpret_cast<char*>(&_currSurrogateBitSize), 4);
        outfile.write(reinterpret_cast<char*>(&surrSize), 4);
        outfile.write(reinterpret_cast<char*>(&typeSize), 1);

        for(auto & e : _surrogates) {
            outfile.write(reinterpret_cast<char*>(&e), 4);
        }

        outfile.write(reinterpret_cast<char*>(&dictSize), 4);
        for(auto & e : _dictionary) {
            outfile.write(reinterpret_cast<char*>(&e), typeSize);
        }


        outfile.flush();
        outfile.close();
        return true;
	}

    template<>
    bool DictionaryCompressedColumn<std::string>::store(const std::string& path_){
        //string path("data/");
        std::string path(path_);
        path += "/";
        path += this->name_;
        //std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
        std::ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);

        uint32_t dictSize = _dictionary.size();
        uint32_t surrSize = _surrogates.size();

        outfile.write(reinterpret_cast<char*>(&_surrogateEntries), 4);
        outfile.write(reinterpret_cast<char*>(&_currSurrogateBitSize), 4);
        outfile.write(reinterpret_cast<char*>(&surrSize), 4);

        for(auto & e : _surrogates) {
            outfile.write(reinterpret_cast<char*>(&e), 4);
        }

        outfile.write(reinterpret_cast<char*>(&dictSize), 4);
        for(auto & e : _dictionary) {
            uint32_t size = e.size();
            outfile.write(reinterpret_cast<char*>(&size), 4);
            outfile.write(e.c_str(), e.size());
        }


        outfile.flush();
        outfile.close();
        return true;
    }

	template<class T>
	bool DictionaryCompressedColumn<T>::load(const std::string& path_){
        std::string path(path_);
        std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
        //string path("data/");
        path += "/";
        path += this->name_;

        //std::cout << "Opening File '" << path << "'..." << std::endl;
        std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);

        uint32_t surrogateListSize = 0, dictionarySize = 0;
        uint8_t typeSize;
        T entry;
        uint32_t ientry;


        infile.read(reinterpret_cast<char*>(&_surrogateEntries), 4);
        infile.read(reinterpret_cast<char*>(&_currSurrogateBitSize), 4);
        infile.read(reinterpret_cast<char*>(&surrogateListSize), 4);
        infile.read(reinterpret_cast<char*>(&typeSize), 1);

        _surrogates.resize(surrogateListSize);
        for(auto & e : _surrogates) {
            infile.read(reinterpret_cast<char*>(&ientry), 4);
            e = ientry;
        }

        infile.read(reinterpret_cast<char*>(&dictionarySize), 4);
        _dictionary.resize(dictionarySize);
        for(auto & e : _dictionary) {
            infile.read(reinterpret_cast<char*>(&entry), typeSize);
            e = entry;
        }

        infile.close();
		return true;
	}
    template<>
    bool DictionaryCompressedColumn<std::string>::load(const std::string& path_){
        std::string path(path_);
        std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
        //string path("data/");
        path += "/";
        path += this->name_;

        //std::cout << "Opening File '" << path << "'..." << std::endl;
        std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);

        uint32_t surrogateListSize = 0, dictionarySize = 0;
        uint32_t ientry;


        infile.read(reinterpret_cast<char*>(&_surrogateEntries), 4);
        infile.read(reinterpret_cast<char*>(&_currSurrogateBitSize), 4);
        infile.read(reinterpret_cast<char*>(&surrogateListSize), 4);

        _surrogates.resize(surrogateListSize);
        for(auto & e : _surrogates) {
            infile.read(reinterpret_cast<char*>(&ientry), 4);
            e = ientry;
        }

        infile.read(reinterpret_cast<char*>(&dictionarySize), 4);
        _dictionary.resize(dictionarySize);
        for(auto & e : _dictionary) {
            infile.read(reinterpret_cast<char*>(&ientry), 4);
            char* buffer = new char[ientry+1];
            buffer[ientry] = 0;

            infile.read(buffer, ientry);
            e = buffer;
            delete [] buffer;
        }

        infile.close();
        return true;
    }





	template<class T>
	T& DictionaryCompressedColumn<T>::operator[](const int tid){
		return _dictionary[_lookupInSurrogates(tid)];
	}

	template<class T>
	unsigned int DictionaryCompressedColumn<T>::getSizeinBytes() const throw(){
	    // wont be correct for std::string...
		return _surrogates.capacity()*sizeof(uint32_t) + _dictionary.capacity()*sizeof(T); //return values_.capacity()*sizeof(T);
	}

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

