from apache_beam import PTransform
from apache_beam import Map
from apache_beam import GroupByKey

class GroupById(PTransform):

    def tag_with_id(self, value):
        return (value.id, value)

    def expand(self, xs):
        return (
            xs
            | Map(self.tag_with_id)
            | GroupByKey()
        )



class GroupByIdAndDate(PTransform):

    def tag_with_id(self, value):
        return (f'{value.id}_{value.timestamp.date()}', value)

    def expand(self, xs):
        return (
            xs
            | Map(self.tag_with_id)
            | GroupByKey()
        )
