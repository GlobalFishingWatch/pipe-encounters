from pipe_encounters.sources import split_id_prefix


def test_split_id_prefix():
    assert split_id_prefix("project.dataset.positions") == ("", "project.dataset.positions")
    assert split_id_prefix("VMS::project.dataset.vms") == ("VMS:", "project.dataset.vms")
