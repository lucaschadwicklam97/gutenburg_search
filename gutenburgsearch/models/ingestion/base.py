from abc import ABC, abstractmethod

class BaseIngestion(ABC):
        
    @abstractmethod
    def extract(self, *args, **kwargs):
        raise NotImplementedError(f'Extract method not implemented by {self.__class__.__name__}')

    @abstractmethod
    def transform(self, *args, **kwargs):
        raise NotImplementedError(f'Ingest method not implemented by {self.__class__.__name__}')

    @abstractmethod
    def load(self, *args, **kwargs):
        raise NotImplementedError(f'Ingest method not implemented by {self.__class__.__name__}')
