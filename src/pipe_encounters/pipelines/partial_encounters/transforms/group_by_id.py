from apache_beam import GroupByKey, Map, PTransform


class GroupByIdAndDate(PTransform):
    def tag_with_id(self, value):
        return (f"{value.id}_{value.timestamp.date()}", value)

    def expand(self, xs):
        return xs | Map(self.tag_with_id) | GroupByKey()
