
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
	    if (powl(2, _currSurrogateBitSize)-1 > _surrogateEntries + numNewEntries) {
	        return;
	    }

        uint8_t newSurrogateSize = static_cast<uint8_t>(
                ceil(log2(static_cast<double>(_surrogateEntries + numNewEntries))));



	    std::vector<uint32_t> newSurrogates;
        for (uint32_t tid = 0;tid<_surrogateEntries;++tid) {
            __insertSurrogate(newSurrogates, _lookupInSurrogates(tid), tid, newSurrogateSize);
        }

        _surrogates = newSurrogates;
        _currSurrogateBitSize = newSurrogateSize;
	}

    template<class T>
    constexpr uint32_t DictionaryCompressedColumn<T>::_bitMask(const uint32_t size) const {
        uint32_t v = 0;
        for(uint32_t i = 0;i<size;++i){
            v |= 1;
            v <<=1;
        }
        return v;
    }

    template<class T>
    uint32_t DictionaryCompressedColumn<T>::_lookupInSurrogates(uint32_t tid) const {
        uint64_t bitAddress = (tid * _currSurrogateBitSize);
        uint8_t bitIndex = bitAddress%32;
        uint32_t byteAddress = bitAddress/32;

        if (byteAddress >= _surrogates.size()) {
            throw std::runtime_error("surrogates out of bounds");
        }

        // split between entry and previous one?
        bool split = bitAddress % 32 < _currSurrogateBitSize;

        if (!split) {
            return (_surrogates[byteAddress] & (_bitMask(_currSurrogateBitSize)<<bitIndex)) >> bitIndex;
        } else {
            uint64_t temp = (static_cast<uint64_t>(_surrogates[byteAddress])<<32) |
                    static_cast<uint64_t>(_surrogates[byteAddress-1]);
            bitIndex = 32-(_currSurrogateBitSize-bitIndex);
            return (temp & (static_cast<uint64_t>(_bitMask(_currSurrogateBitSize))<<bitIndex)) >> bitIndex;
        }
    }

    template<class T>
    void DictionaryCompressedColumn<T>::__insertSurrogate(std::vector<uint32_t>& target, uint32_t value,
            uint32_t tid, uint8_t entrysize) const
    {
        uint64_t bitAddress = (tid * entrysize);
        uint8_t bitIndex = bitAddress%32;
        uint32_t byteAddress = bitAddress/32;

        if (byteAddress >= target.size()) {
            target.emplace_back(0);
        }

        // split between entry and previous one?
        bool split = bitAddress % 32 < entrysize;

        if (!split) {
            target[byteAddress] &= ~(_bitMask(entrysize)<<(bitIndex));
            target[byteAddress] |= (value<<bitIndex);
        } else {
            uint64_t temp = (static_cast<uint64_t>(target[byteAddress])<<32) |
                            static_cast<uint64_t>(target[byteAddress-1]);

            bitIndex = 32-(entrysize-bitIndex);

            temp &= ~(_bitMask(entrysize)<<(bitIndex));
            temp |= (value<<bitIndex);

            target[byteAddress] = static_cast<uint32_t>(temp>>32);
            target[byteAddress-1] = static_cast<uint32_t>(temp&32);
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
	        _dictionary.emplace_back(value);
            dictIndex = _dictionary.size()-1;
	    }

	    // add surrogate
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

	    for(uint32_t tid=0;tid<_surrogateEntries;++tid) {
	        std::cout<<"tid= "<<tid<<"\tvalue="<<_dictionary[_lookupInSurrogates(tid)];
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

        outfile<<_surrogateEntries;
        outfile<<_surrogates.size();
        for(const auto & e : _surrogates) {
            outfile<<e;
        }
        outfile<<_dictionary.size();
        for(const auto & e : _dictionary) {
            outfile<<e;
        }


        outfile.flush();
        outfile.close();
        return true;
	}
	template<class T>
	bool DictionaryCompressedColumn<T>::load(const std::string& path_){
        std::string path(path_);
        //std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
        //string path("data/");
        path += "/";
        path += this->name_;

        //std::cout << "Opening File '" << path << "'..." << std::endl;
        std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);
        boost::archive::binary_iarchive ia(infile);

        uint32_t surrogateListSize = 0, dictionarySize = 0;
        ia >> _surrogateEntries;
        ia >> surrogateListSize;

        _surrogates.resize(surrogateListSize);
        for(uint32_t i=0;i<surrogateListSize;++i) {
            ia >> _surrogates[i];
        }

        ia >>dictionarySize;
        _dictionary.resize(dictionarySize);

        for(uint32_t i=0;i<dictionarySize;++i) {
            ia >> _dictionary[i];
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

