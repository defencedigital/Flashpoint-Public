

class Entity:
    """ Entity class.

    Instances of the entity class are used to represent the CDF entities.
    Model processor scripts use the functions and methods of an instance of the Dataset class to generate and
    manipulate an array of instances of the entity class. They should not require direct creation or manipulation.
    Events involving a particular entity are identified via the uid for that entity instance.
    """

    def __init__(self, uid: str, unit_name: str = None, unit_type: str = None,
                 commander: str = None, level: str = None,
                 affiliation: str = None, force: str = None,
                 init_comps: int = None, cbt_per_comp: int = None,
                 system_entity: bool = False,
                 start_entity: bool = True, add_time: float = 0.0) -> None:
        """ Entity class init method.

        Args:
            uid: unique identifier for this entity instance
            unit_name: name of this entity instance (optional, default None)
            unit_type: type of this entity instance (optional, default None)
            commander: uid of the entity instance which is commander of this entity instance (optional, default None)
            level: level of the entity instance in its organisation (optional, default None)
            affiliation: affiliation of this entity instance (optional, default None)
            force: force to which the affiliation of this entity instance belongs (optional, default None)
            init_comps: initial components of this entity instance (optional, default None)
            cbt_per_comp: combat power per component of this entity instance (optional, default None)
            start_entity: was entity present from start of game / simulation (optional, default True)
            add_time: game / simulation time that the entity first appears in the output (optional, default 0.0)
        """
        self.uid = uid
        self.unit_name = unit_name
        self.unit_type = unit_type
        self.commander = commander
        self.level = level
        self.affiliation = affiliation
        self.force = force
        self.init_comps = init_comps
        self.cbt_per_comp = cbt_per_comp
        self.system_entity = system_entity
        self.start_entity = start_entity
        self.add_time = add_time

        # dictionary to keep track of event ids for this entity
        self.entity_event_id_dict = {'evn_ser': [],
                                     'evn_id': [],
                                     'type': [],
                                     'prim_uid': [],
                                     'sec_uid': [],
                                     'data_idx': []}

        # lists to hold event data for this entity
        self.location_time = []
        self.location_x = []
        self.location_y = []
        self.location_detail = []

        self.shots_time = []
        self.shots_detail = []

        self.kills_time = []
        self.kills_victim = []
        self.kills_detail = []

        self.losses_time = []
        self.losses_killer = []
        self.losses_detail = []

        self.spot_time = []
        self.spot_entity = []
        self.spot_detail = []

        self.seen_time = []
        self.seen_entity = []
        self.seen_detail = []

        self.stop_time = []
        self.stop_entity = []
        self.stop_detail = []

        self.state_time = []
        self.state_detail = []

    def export_entity_dict(self) -> dict:
        """
        Export a dict with the entities parameters and any data added
        """
        return_dict = {}
        for key in vars(self):
            return_dict[key] = vars(self)[key]

        return return_dict

    def import_entity_dict(self, load_vars_dict):
        """
        Set the entities parameters and add data from a dict exported from the get_data_dict function
        """
        for key in load_vars_dict:
            setattr(self, key, load_vars_dict[key])
