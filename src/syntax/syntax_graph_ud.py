from .syntax_graph import SyntaxGraph


class UDSyntaxGraph(SyntaxGraph):
    """
    SyntaxGraph variant for corpora whose feats use Universal Dependencies
    style values (e.g. Case=Nom, Number=Sing) instead of the lowercase
    EstCG-style codes ("nom", "sg", ...) that the base class expects.

    get_node_case/get_node_number are overridden to return the raw UD
    Case=/Number= values directly (e.g. "Nom", "Par", "Sing", "Plur") -
    callers should filter/compare using UD codes, not EstCG ones.
    """

    def get_node_case(self, node_id, not_null=True):
        """
        Korpuse feats kasutavad UD vorme (nt Case=Nom). Tagastab UD
        käändekoodi otse (https://universaldependencies.org/u/feat/Case.html).
        """
        feats = self.nodes[node_id]["feats"] or {}
        case = feats.get("Case")
        if case:
            return case
        if not_null:
            return "<puudub>"
        return None

    def get_node_number(self, node_id):
        """
        Korpuse feats kasutavad UD vorme (nt Number=Sing). Tagastab UD
        arvukoodi otse ("Sing"/"Plur").
        """
        feats = self.nodes[node_id]["feats"] or {}
        return feats.get("Number")
